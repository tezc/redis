#!/bin/bash
# PGO training workload. Run by src/Makefile against a build instrumented with
# -fprofile-generate; the profile is written when each process exits.
#
# usage: pgo-train.sh <redis-server> <redis-cli> <redis-benchmark>
#
# The goal is to exercise the code that matters for performance in production,
# weighted roughly by how common it is, and to touch every internal encoding of
# every data type (code that is never executed during training is optimized for
# size, so missing coverage can make that code slower than a regular build).
#
#   commands     the most used commands of every type, in three weight tiers,
#                pipelined and not pipelined, RESP2 and RESP3, with and
#                without io-threads; hits and misses
#   encodings    int / embstr / raw strings, values up to 16KB; small (listpack,
#                intset) and big (hashtable, skiplist, quicklist) collections;
#                arrays, native bitmaps, set members with TTL
#   expiration   TTL commands, lazy and active expiry, hash field expiration
#   eviction     maxmemory with LRU and LFU policies
#   scripting    EVALSHA, functions, MULTI/EXEC, pub/sub with subscribers
#   bulk load    HIMPORT (fieldset based hash import)
#   other        blocking pops, ACL restricted user, client handshake and
#                tracking, MONITOR, hotkeys tracking, dump / restore
#   persistence  SAVE, BGSAVE, RDB load, AOF (append, rewrite, load)
#   replication  diskless full sync on master and replica (disk-backed and
#                diskless load), partial resync, applying the write stream
#   cluster      commands, redirections, atomic slot migration (both sides),
#                cluster-mode RDB load and replica sync
#
# Everything runs on loopback with small datasets (about a minute). Exits
# non-zero if the core (standalone) part fails, the build then falls back to a
# regular build. The other parts are best effort.

SERVER=$1; CLI=$2; BENCH=$3
[ -x "$SERVER" ] && [ -x "$CLI" ] && [ -x "$BENCH" ] || { echo "usage: $0 <redis-server> <redis-cli> <redis-benchmark>" >&2; exit 1; }
set -f

TMP=$(mktemp -d "${TMPDIR:-/tmp}/redis-pgo.XXXXXX") || exit 1
PIDS=""
cleanup() { for p in $PIDS; do kill -9 "$p" 2>/dev/null; done; rm -rf "$TMP"; }
trap cleanup EXIT
note() { echo "pgo-train: $*" >&2; }
phase() { [ -n "$PGO_TRAIN_VERBOSE" ] && note "${SECONDS}s: $*"; return 0; }
fail() { note "$*"; for l in "$TMP"/*/log; do [ -f "$l" ] && { echo "--- $l" >&2; tail -5 "$l" >&2; }; done; exit 1; }

TO=""; command -v timeout >/dev/null 2>&1 && TO="timeout 120"
# Ports (and cluster bus ports, +10000) are kept below the ephemeral port range,
# otherwise they can collide with the client side of a benchmark connection.
BASE=$((11000 + ($$ % 9000)))
P_M=$BASE; P_R1=$((BASE+1)); P_R2=$((BASE+2)); P_E=$((BASE+3)); P_A=$((BASE+4))
P_C1=$((BASE+5)); P_C2=$((BASE+6)); P_C3=$((BASE+7))

cli() { local port=$1; shift; $TO "$CLI" -p "$port" "$@" 2>/dev/null; }

# start <name> <port> [server args...]
start() {
    local name=$1 port=$2 i=0; shift 2
    mkdir -p "$TMP/$name"
    "$SERVER" --port "$port" --bind 127.0.0.1 --dir "$TMP/$name" --logfile "$TMP/$name/log" \
        --save "" --appendonly no --repl-diskless-sync-delay 0 "$@" &
    local pid=$!
    PIDS="$PIDS $pid"; eval "PID_$name=$pid"
    until [ "$(cli "$port" ping)" = PONG ]; do
        i=$((i+1)); [ $i -gt 600 ] && return 1
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 0.1
    done
}

# stop <name> <port>: clean shutdown, so that the profile is written
stop() {
    local pid i=0; eval "pid=\$PID_$1"
    [ -n "$pid" ] || return 0
    cli "$2" shutdown nosave >/dev/null
    while kill -0 "$pid" 2>/dev/null; do i=$((i+1)); [ $i -gt 300 ] && return 1; sleep 0.1; done
}

# bench <port> <pipeline> <requests> <benchmark args / command...>
bench() {
    local port=$1 pipe=$2 n=$3; shift 3
    $TO "$BENCH" -p "$port" -q -c 20 -P "$pipe" -n "$n" -r 20000 "$@" >/dev/null 2>&1
}

# script <port>: commands from stdin, one per line (for sequences and quoting)
script() { $TO "$CLI" -p "$1" >/dev/null 2>&1; }
# rep <count> <line>...: print the lines <count> times
rep() { local n=$1 i=0; shift; while [ $i -lt "$n" ]; do printf '%s\n' "$@"; i=$((i+1)); done; }

# wait_for <tries> <command...>: retry every 100ms until the command succeeds
wait_for() { local n=$1 i=0; shift; until "$@"; do i=$((i+1)); [ $i -gt "$n" ] && return 1; sleep 0.1; done; }
link_up() { cli "$1" info replication | grep -q master_link_status:up; }
bgsave_done() { cli "$1" info persistence | grep -q rdb_bgsave_in_progress:0; }
rewrite_done() { cli "$1" info persistence | grep -q aof_rewrite_in_progress:0; }
cluster_ok() { cli "$1" cluster info | grep -q cluster_state:ok; }
offset() { cli "$1" info replication | grep ^master_repl_offset | tr -d '\r' | cut -d: -f2; }
caught_up() { [ "$(offset "$2")" -ge "$(offset "$1")" ] 2>/dev/null; }
asm_done() { cli "$1" cluster migration status all | grep -A1 -x state | grep -qE 'completed|failed'; }

V=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx     # 48 bytes: raw string
S=xxxxxxxxxxxxxxxx                                     # 16 bytes: embstr
LONG=$V$V$V$V$V$V$V$V; LONG=$LONG$LONG$LONG            # 1152 bytes: long member

# ------------------------------------------------------------------ commands
# "<tier> <command>". Tier 1: the commands that dominate real workloads.
# Tier 2: common. Tier 3: coverage. __rand_int__ is a random 12 digit number
# below 20000. Keys named s* are small collections (listpack / intset), keys
# named b* are a few big ones (hashtable / skiplist / quicklist). All of these
# are single-key commands, so the same list is used in cluster mode.
CMDS="
1 set key:__rand_int__ $V
1 get key:__rand_int__
1 get nokey:__rand_int__
1 set skey:__rand_int__ $S
1 get skey:__rand_int__
1 set ikey:__rand_int__ 1234567
1 get ikey:__rand_int__
1 setex ekey:__rand_int__ 100000 $V
1 set lock:__rand_int__ $S nx ex 100000
1 incr counter:__rand_int__
1 incrby counter:__rand_int__ 7
1 del lock:__rand_int__
1 exists key:__rand_int__
1 expire key:__rand_int__ 100000
1 ttl ekey:__rand_int__
1 hset sh:__rand_int__ f1 $V f2 $S f3 12345
1 hget sh:__rand_int__ f2
1 hmget sh:__rand_int__ f1 f3 nofield
1 hgetall sh:__rand_int__
1 hincrby sh:__rand_int__ cnt 1
1 hset bh:1 f:__rand_int__ $V
1 hget bh:1 f:__rand_int__
1 hset bh:2 f:__rand_int__ $S
1 hmget bh:2 f:__rand_int__ f:__rand_int__ f:__rand_int__
1 lpush q:__rand_int__ $V
1 rpop q:__rand_int__
1 rpush q:__rand_int__ $V
1 lpop q:__rand_int__
1 lpush bl:1 $V
1 lrange bl:1 0 9
1 sadd ss:__rand_int__ a b c
1 sismember ss:__rand_int__ b
1 smembers ss:__rand_int__
1 sadd bs:1 member:__rand_int__
1 sismember bs:1 member:__rand_int__
1 srem bs:1 member:__rand_int__
1 zadd sz:__rand_int__ 1 a 2 b 3 c
1 zrange sz:__rand_int__ 0 -1
1 zscore sz:__rand_int__ b
1 zadd bz:1 __rand_int__ member:__rand_int__
1 zincrby bz:1 1 member:__rand_int__
1 zscore bz:1 member:__rand_int__
1 zrange bz:1 0 9
1 zrevrange bz:1 0 9 withscores
1 zrangebyscore bz:1 1000 2000 limit 0 10
1 zrank bz:1 member:__rand_int__
1 xadd bst:1 * field $V
1 publish channel $V
1 ping
2 setnx nx:__rand_int__ $V
2 getset key:__rand_int__ $V
2 getdel nx:__rand_int__
2 getex ekey:__rand_int__ ex 100000
2 append akey:__rand_int__ xxxxxxxx
2 strlen akey:__rand_int__
2 getrange akey:__rand_int__ 0 15
2 setrange akey:__rand_int__ 4 yyyy
2 decr counter:__rand_int__
2 incrbyfloat fcounter:__rand_int__ 1.5
2 psetex ekey:__rand_int__ 100000000 $V
2 pexpire key:__rand_int__ 100000000
2 pttl ekey:__rand_int__
2 persist ekey:__rand_int__
2 type key:__rand_int__
2 unlink ekey:__rand_int__
2 touch key:__rand_int__
2 hexists sh:__rand_int__ f1
2 hlen sh:__rand_int__
2 hkeys sh:__rand_int__
2 hvals sh:__rand_int__
2 hsetnx sh:__rand_int__ f4 $S
2 hdel sh:__rand_int__ f4
2 hstrlen sh:__rand_int__ f1
2 hexists bh:1 f:__rand_int__
2 hincrby bh:1 f:__rand_int__:n 1
2 hdel bh:2 f:__rand_int__
2 hscan bh:1 0 count 10
2 hrandfield bh:1
2 hsetex sh:__rand_int__ ex 100000 fields 1 f5 $S
2 hgetex sh:__rand_int__ ex 100000 fields 1 f1
2 hgetdel sh:__rand_int__ fields 1 f5
2 hexpire sh:__rand_int__ 100000 fields 1 f2
2 httl sh:__rand_int__ fields 1 f2
2 hpersist sh:__rand_int__ fields 1 f2
2 llen bl:1
2 lset bl:1 5 $V
2 rpoplpush bl:1 bl:1
3 lpop bl:1 3
2 lindex bl:1 500
2 lrange bl:1 0 99
2 ltrim bl:1 0 19999
2 lrange q:__rand_int__ 0 -1
2 scard ss:__rand_int__
2 srandmember ss:__rand_int__
2 smismember ss:__rand_int__ a x
2 spop ss:__rand_int__
2 srem ss:__rand_int__ c
2 sscan bs:1 0 count 10
2 scard bs:1
2 srandmember bs:1 3
2 sadd is:__rand_int__ 1 2 3 4
2 sismember is:__rand_int__ 3
2 sadd bis:1 __rand_int__
2 sismember bis:1 __rand_int__
2 zcard bz:1
2 zcount bz:1 1000 5000
2 zrevrank bz:1 member:__rand_int__
2 zmscore bz:1 member:__rand_int__ member:__rand_int__
2 zrevrangebyscore bz:1 2000 1000 limit 0 10
2 zrange bz:1 1000 2000 byscore limit 0 10
2 zrem bz:1 member:__rand_int__
3 zremrangebyrank bz:1 0 0
3 zpopmin bz:1
3 zpopmax bz:1
2 zscan bz:1 0 count 10
2 zrank sz:__rand_int__ c
2 zincrby sz:__rand_int__ 1 a
2 zrem sz:__rand_int__ c
2 zrangebyscore sz:__rand_int__ 0 10
2 xadd cst:1 maxlen ~ 1000 * field $V
2 xlen bst:1
2 xrange bst:1 - + count 10
2 xrevrange bst:1 + - count 10
2 xread count 10 streams bst:1 0
2 xtrim bst:1 maxlen ~ 10000
2 pfadd hll:__rand_int__ e:__rand_int__
2 pfcount hll:__rand_int__
2 pfadd bhll:1 e:__rand_int__
2 pfcount bhll:1
2 setbit bits:__rand_int__ 4000 1
2 getbit bits:__rand_int__ 4000
2 bitcount bits:__rand_int__
2 decrby counter:__rand_int__ 3
2 increx rl:__rand_int__ byint 1 ex 100000
2 delex lock:__rand_int__ ifeq $S
2 hmset sh:__rand_int__ f1 $V f2 $S
2 saddex ts:__rand_int__ ex 100000 members 2 a b
2 sexpire ts:__rand_int__ 100000 members 1 a
2 sttl ts:__rand_int__ members 1 a
2 spersist ts:__rand_int__ members 1 a
2 sismember ts:__rand_int__ b
2 arset smallarr:__rand_int__ 0 $S $S $S 12345
2 arget smallarr:__rand_int__ 2
2 armget smallarr:__rand_int__ 0 1 3 9
2 argetrange smallarr:__rand_int__ 0 3
2 arinsert smallarr:__rand_int__ $S
2 arlen smallarr:__rand_int__
2 arset bigarr:1 __rand_int__ $V
2 arget bigarr:1 __rand_int__
2 armset bigarr:1 __rand_int__ $S __rand_int__ $S
2 armget bigarr:1 __rand_int__ __rand_int__ __rand_int__
2 arinsert bigarr:2 $V
2 arring ring:1 1000 $V
2 argetrange bigarr:1 1000 1010
2 arscan bigarr:1 0 20000 limit 10
2 arlastitems bigarr:2 10
2 arcount bigarr:1
2 ardel bigarr:1 __rand_int__
3 arop bigarr:3 0 20000 sum
3 arset bigarr:3 __rand_int__ 12345
3 argrep bigarr:1 0 2000 exact $V limit 5
3 ardelrange bigarr:2 0 0
3 arnext bigarr:2
3 arseek ring:1 5
3 arinfo bigarr:1
3 hincrbyfloat sh:__rand_int__ fl 1.5
3 hpexpire sh:__rand_int__ 100000000 fields 1 f1
3 hpttl sh:__rand_int__ fields 1 f1
3 hexpiretime sh:__rand_int__ fields 1 f1
3 lpushx q:__rand_int__ $V
3 rpushx q:__rand_int__ $V
3 lmpop 1 q:__rand_int__ left count 2
3 zmpop 1 sz:__rand_int__ min
3 zrangebylex sz:__rand_int__ - +
3 xautoclaim bst:1 grp c3 0 0-0 count 10
3 xinfo stream bst:1
3 digest key:__rand_int__
3 bitfield_ro bf:__rand_int__ get u8 0
3 bitrangeintarray bits:__rand_int__ 0 -1
3 pexpireat key:__rand_int__ 4102444800000
3 geodist geo:1 m:__rand_int__ m:__rand_int__
3 bitpos bits:__rand_int__ 1
3 bitfield bf:__rand_int__ incrby u8 0 1
3 geoadd geo:1 13.361389 38.115556 m:__rand_int__
3 geopos geo:1 m:__rand_int__
3 geosearch geo:1 fromlonlat 13.36 38.11 byradius 10 km count 10
3 object encoding key:__rand_int__
3 memory usage sh:__rand_int__
3 lpos bl:1 $V rank 1 maxlen 100
3 linsert q:__rand_int__ before nosuch $V
3 lrem q:__rand_int__ 1 nosuch
3 zrandmember bz:1
3 zlexcount sz:__rand_int__ - +
3 zremrangebyscore bz:1 0 0
3 expireat key:__rand_int__ 4102444800
3 expiretime key:__rand_int__
3 echo $V
"
# Keyless / multi-key commands: standalone only.
CMDS_STANDALONE="
1 mget key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__
1 mset mk:__rand_int__ $V mk:__rand_int__ $V mk:__rand_int__ $V
1 del key:__rand_int__ nokey:__rand_int__
1 scan 0 count 10
2 msetex 2 mk:__rand_int__ $V mk:__rand_int__ $V ex 100000
2 scan 0 match key:* count 100
2 exists key:__rand_int__ nokey:__rand_int__ skey:__rand_int__
2 dbsize
2 randomkey
2 sinter ss:__rand_int__ ss:__rand_int__
2 sunion ss:__rand_int__ ss:__rand_int__
2 sdiff ss:__rand_int__ ss:__rand_int__
2 zunionstore zdst:__rand_int__ 2 sz:__rand_int__ sz:__rand_int__
2 zinterstore zdst:__rand_int__ 2 sz:__rand_int__ sz:__rand_int__
2 lmove q:__rand_int__ q:__rand_int__ right left
2 smove ss:__rand_int__ ss:__rand_int__ a
3 copy key:__rand_int__ cp:__rand_int__ replace
3 msetnx mnx:__rand_int__ $S mnx:__rand_int__ $S
3 sinterstore sdst:__rand_int__ ss:__rand_int__ ss:__rand_int__
3 sunionstore sdst:__rand_int__ ss:__rand_int__ ss:__rand_int__
3 zinter 2 sz:__rand_int__ sz:__rand_int__
3 zunion 2 sz:__rand_int__ sz:__rand_int__ withscores
3 zdiff 2 sz:__rand_int__ sz:__rand_int__
3 zintercard 2 sz:__rand_int__ sz:__rand_int__
3 zrangestore zdst:__rand_int__ bz:1 0 9
3 sort is:__rand_int__
3 role
3 sintercard 2 ss:__rand_int__ ss:__rand_int__
3 pfmerge hlldst hll:__rand_int__ hll:__rand_int__
3 bitop and bdst bits:__rand_int__ bits:__rand_int__
3 info
3 time
"
# MGET with many keys: more keys than one prefetch batch.
MK=""; i=0; while [ $i -lt 30 ]; do MK="$MK key:__rand_int__"; i=$((i+1)); done
CMDS_STANDALONE="$CMDS_STANDALONE
2 mget$MK
2 mget$MK$MK$MK key:__rand_int__ nokey:__rand_int__
"

# commands <port> <list> <scale> [max tier]: each command pipelined, tier 1
# also not pipelined
commands() {
    local port=$1 scale=$3 max=${4:-3} tier line n total=0 failed=0
    while read -r tier line; do
        [ -z "$tier" ] && continue
        [ "$tier" -gt "$max" ] && continue
        case $tier in 1) n=30000;; 2) n=8000;; *) n=2000;; esac
        n=$((n * scale / 100)); total=$((total+1))
        # redis-benchmark stops at the first error reply (expected e.g. for
        # writes on a replica or redirections in a cluster).
        if bench "$port" 16 $n $line; then
            [ "$tier" = 1 ] && bench "$port" 1 $((n / 10)) $line
        else
            failed=$((failed+1)); [ -n "$PGO_TRAIN_VERBOSE" ] && note "error reply: $line"
        fi
    done <<EOF
$2
EOF
    [ $((failed * 4)) -lt $total ]      # fails if a quarter of the commands failed
}

# strings <port>: -t set/get with small to large values
strings() {
    bench "$1" 16 100000 -r 100000 -d 64 -t set,get || return 1
    bench "$1" 16 30000 -r 30000 -d 512 -t set,get || return 1
    bench "$1" 16 20000 -r 20000 -d 1024 -t set,get || return 1
    bench "$1" 16 8000 -r 8000 -d 4096 -t set,get || return 1
    bench "$1" 4 2000 -r 2000 -d 16384 -t set,get || return 1
    bench "$1" 1 5000 -r 100000 -d 64 -t set,get || return 1
    # The values above are a single repeated byte. Add values that look like
    # real data, otherwise RDB compression is trained only on the best case:
    # text made of words (16 bytes - 4KB) and, one in eight, random bytes.
    local sha
    sha=$(cli "$1" script load 'local w={} math.randomseed(7)
for i=1,128 do local t={} for j=1,math.random(3,10) do t[j]=string.char(96+math.random(26)) end w[i]=table.concat(t) end
math.randomseed(tonumber(ARGV[1]))
local sizes={16,64,256,512,1024,4096} local len=sizes[math.random(6)] local t,l={},0
if math.random(8)==1 then
  while l<len do l=l+1 t[l]=string.char(math.random(0,255)) end
  return redis.call("set",KEYS[1],table.concat(t))
end
while l<len do local s=w[math.random(128)] t[#t+1]=s l=l+#s+1 end
return redis.call("set",KEYS[1],table.concat(t," "))')
    [ -n "$sha" ] && bench "$1" 16 30000 -r 1000000 evalsha "$sha" 1 txt:__rand_int__ __rand_int__
    return 0
}

# extras <port>: things redis-benchmark cannot express
extras() {
    local port=$1 sha sub1 sub2
    # fieldset based hash import (session state, so one connection), then
    # reads and updates of the imported hashes
    awk -v v="$V" -v s="$S" 'BEGIN { print "himport prepare pgofs f1 f2 f3 f4 f5";
        for (i = 0; i < 30000; i++) printf "himport set th:%012d pgofs %s %s 12345 %s %d\n", i % 10000, v, s, v, i;
        print "himport discard pgofs" }' | $TO "$CLI" -p "$port" --pipe >/dev/null 2>&1
    bench "$port" 16 20000 -r 10000 hget th:__rand_int__ f2
    bench "$port" 16 10000 -r 10000 hmget th:__rand_int__ f1 f3 f5
    bench "$port" 16 10000 -r 10000 hgetall th:__rand_int__
    bench "$port" 16 10000 -r 10000 hset th:__rand_int__ f3 $S
    bench "$port" 16 5000 -r 10000 hdel th:__rand_int__ f4
    # transactions
    rep 1500 "multi" "incr txcounter" "set txkey $V" "get txkey" "exec" | script "$port"
    rep 300 "watch txkey" "multi" "set txkey $S" "exec" | script "$port"
    # Lua scripts and functions
    sha=$(cli "$port" script load "redis.call('set',KEYS[1],ARGV[1]) return redis.call('get',KEYS[1])")
    [ -n "$sha" ] && { bench "$port" 16 10000 evalsha "$sha" 1 lua:__rand_int__ $V; bench "$port" 1 1000 evalsha "$sha" 1 lua:__rand_int__ $V; }
    sha=$(cli "$port" script load "local n=redis.call('incr',KEYS[1]) if n==1 then redis.call('expire',KEYS[1],100000) end return n")
    [ -n "$sha" ] && bench "$port" 16 10000 evalsha "$sha" 1 rate:__rand_int__
    cli "$port" function load replace "#!lua name=pgolib
redis.register_function('pgoset', function(keys, args) redis.call('hset', keys[1], 'f', args[1]) return redis.call('hget', keys[1], 'f') end)" >/dev/null &&
        bench "$port" 16 5000 fcall pgoset 1 fn:__rand_int__ $V
    # consumer groups: read and acknowledge
    cli "$port" xgroup create bst:1 grp 0 >/dev/null
    sha=$(cli "$port" script load "local r=redis.call('xreadgroup','group','grp','c1','count','10','streams',KEYS[1],'>') if r then for _,e in ipairs(r[1][2]) do redis.call('xack',KEYS[1],'grp',e[1]) end end return 1")
    [ -n "$sha" ] && bench "$port" 16 2000 evalsha "$sha" 1 bst:1
    bench "$port" 16 2000 xreadgroup group grp c2 count 5 streams bst:1 ">"
    bench "$port" 16 500 xpending bst:1 grp
    # pub/sub with subscribers attached
    "$CLI" -p "$port" subscribe channel >/dev/null 2>&1 & sub1=$!
    "$CLI" -p "$port" psubscribe "chan*" >/dev/null 2>&1 & sub2=$!
    PIDS="$PIDS $sub1 $sub2"; sleep 0.2
    bench "$port" 16 20000 publish channel $V; bench "$port" 1 2000 publish channel $V
    kill "$sub1" "$sub2" 2>/dev/null
    # lazy and active expiry
    bench "$port" 16 30000 -r 30000 set tmp:__rand_int__ $V px 150
    bench "$port" 16 30000 -r 30000 hset tmph:__rand_int__ f $V
    bench "$port" 16 30000 -r 30000 pexpire tmph:__rand_int__ 150
    sleep 0.4
    bench "$port" 16 30000 -r 30000 get tmp:__rand_int__
    # blocking pops served by pushes (job queue pattern)
    bench "$port" 1 5000 brpop wq 5 & local bp=$!
    sleep 0.2; bench "$port" 1 6000 lpush wq $V; wait $bp
    bench "$port" 1 2000 bzpopmin wz 5 & bp=$!
    sleep 0.2; bench "$port" 1 2500 zadd wz __rand_int__ member:__rand_int__; wait $bp
    # EVAL with the script body (script cache lookup), read-only variants
    rep 2000 "eval \"return redis.call('get',KEYS[1])\" 1 key:000000000042" | script "$port"
    rep 300 "eval_ro \"return redis.call('hget',KEYS[1],'f1')\" 1 sh:000000000042" | script "$port"
    # streams: add + delete, consumer management
    sha=$(cli "$port" script load "local id=redis.call('xadd',KEYS[1],'*','f',ARGV[1]) return redis.call('xdel',KEYS[1],id)")
    [ -n "$sha" ] && bench "$port" 16 3000 evalsha "$sha" 1 dst:1 $V
    sha=$(cli "$port" script load "local r=redis.call('xreadgroup','group','grp','c4','count','5','streams',KEYS[1],'>') if r then for _,e in ipairs(r[1][2]) do redis.call('xackdel',KEYS[1],'grp','ids',1,e[1]) end end return 1")
    [ -n "$sha" ] && bench "$port" 16 1000 evalsha "$sha" 1 bst:1
    rep 50 "xinfo groups bst:1" "xinfo consumers bst:1 grp" "xgroup createconsumer bst:1 grp tmpc" "xgroup delconsumer bst:1 grp tmpc" | script "$port"
    # sharded pub/sub
    "$CLI" -p "$port" ssubscribe schannel >/dev/null 2>&1 & sub1=$!
    PIDS="$PIDS $sub1"; sleep 0.2
    bench "$port" 16 5000 spublish schannel $V
    kill "$sub1" 2>/dev/null
    # rename, dump / restore, databases
    rep 500 "set rk $V" "rename rk rk2" "renamenx rk2 rk" | script "$port"
    sha=$(cli "$port" script load "return redis.call('restore',KEYS[2],0,redis.call('dump',KEYS[1]),'replace')")
    [ -n "$sha" ] && { bench "$port" 16 2000 evalsha "$sha" 2 key:__rand_int__ rst:__rand_int__; bench "$port" 16 2000 evalsha "$sha" 2 sh:__rand_int__ rst:__rand_int__
        bench "$port" 16 2000 evalsha "$sha" 2 sz:__rand_int__ rst:__rand_int__; bench "$port" 16 20 evalsha "$sha" 2 bz:1 rst:bz; }
    rep 300 "select 1" "set dbkey $V" "get dbkey" "select 0" | script "$port"
    # connection handshake of modern clients, client side caching
    local i=0; while [ $i -lt 100 ]; do
        printf '%s\n' "hello 3" "client setinfo lib-name pgo" "client setinfo lib-ver 1.0" "client setname pgo" "ping" | script "$port"; i=$((i+1)); done
    { echo "hello 3"; echo "client tracking on"; rep 2000 "get key:000000000042" "get skey:000000000043"; } | script "$port"
    # an authenticated user with key and command restrictions (ACL checks)
    cli "$port" acl setuser pgouser on ">pgopass" "~key:*" "~sh:*" "+@read" "+@write" "+ping" >/dev/null
    bench "$port" 16 20000 --user pgouser -a pgopass set key:__rand_int__ $V
    bench "$port" 16 20000 --user pgouser -a pgopass get key:__rand_int__
    bench "$port" 16 10000 --user pgouser -a pgopass hget sh:__rand_int__ f1
    bench "$port" 1 2000 --user pgouser -a pgopass get key:__rand_int__
    cli "$port" acl deluser pgouser >/dev/null
    # commands with MONITOR attached and with hotkeys tracking enabled
    "$CLI" -p "$port" monitor >/dev/null 2>&1 & sub1=$!
    PIDS="$PIDS $sub1"; sleep 0.2
    bench "$port" 16 10000 set key:__rand_int__ $V; bench "$port" 16 10000 get key:__rand_int__
    kill "$sub1" 2>/dev/null
    cli "$port" hotkeys start metrics 2 cpu net >/dev/null
    bench "$port" 16 20000 set key:__rand_int__ $V; bench "$port" 16 20000 get key:__rand_int__
    bench "$port" 16 10000 hget sh:__rand_int__ f1
    cli "$port" hotkeys get >/dev/null; cli "$port" hotkeys stop >/dev/null
    # native bitmaps, set member expiry
    rep 200 "setbit nbits 100 1" "setbit nbits 70000 1" "getbit nbits 100" "bitcount nbits" | script "$port"
    bench "$port" 16 20000 -r 20000 saddex tmps:__rand_int__ px 150 members 2 a b
    sleep 0.3
    # full reads of big collections
    bench "$port" 1 50 hgetall bh:2; bench "$port" 1 50 smembers bis:1; bench "$port" 1 20 lrange bl:1 0 -1
    # big bitmap: BITCOUNT over megabytes (the popcount loops only get hot
    # with large inputs), random start offset
    cli "$port" setbit bigbits 33554431 1 >/dev/null
    bench "$port" 16 20000 -r 33554432 setbit bigbits __rand_int__ 1
    sha=$(cli "$port" script load "return redis.call('bitcount',KEYS[1],tonumber(ARGV[1]),-1)")
    [ -n "$sha" ] && bench "$port" 4 400 -r 4194304 evalsha "$sha" 1 bigbits __rand_int__
    bench "$port" 1 50 bitcount bigbits
    # SCAN from random cursors returning many keys per call, type filter
    bench "$port" 16 2000 -r 1000000 scan __rand_int__ count 500
    bench "$port" 16 300 -r 1000000 scan __rand_int__ count 5000
    bench "$port" 16 2000 -r 1000000 scan __rand_int__ count 100 type hash
    bench "$port" 16 1000 scan 0 type hash
    # sorted sets: union / intersection of skiplist encoded sets, copying a
    # big range, long members
    bench "$port" 16 3000 -r 300 zadd zu:1 __rand_int__ member:__rand_int__
    bench "$port" 16 3000 -r 300 zadd zu:2 __rand_int__ member:__rand_int__
    bench "$port" 16 1000 zunion 2 zu:1 zu:2
    bench "$port" 16 1000 zunionstore zudst 2 zu:1 zu:2
    bench "$port" 16 500 zinterstore zidst 2 zu:1 zu:2
    bench "$port" 1 50 zrangestore zrdst bz:1 0 -1
    bench "$port" 16 20000 -r 20000 zadd bzl:1 __rand_int__ $LONG:__rand_int__
    bench "$port" 16 5000 zrevrangebyscore bzl:1 20000 1 withscores limit 1 10
    bench "$port" 1 1000 zrevrangebyscore bzl:1 20000 1 withscores limit 1 10
    bench "$port" 16 5000 -r 20000 zrank bzl:1 $LONG:__rand_int__
    bench "$port" 16 5000 -r 1000 zadd z1k:1 __rand_int__ member:__rand_int__
    bench "$port" 16 5000 zscan z1k:1 0; bench "$port" 1 1000 zscan z1k:1 0
    # pub/sub fan-out: 10 channels, 40 subscribers on each
    local chans="" subs="" i=0
    while [ $i -lt 10 ]; do chans="$chans $(printf 'fan:%012d' $i)"; i=$((i+1)); done
    i=0; while [ $i -lt 40 ]; do
        "$CLI" -p "$port" subscribe $chans >/dev/null 2>&1 & subs="$subs $!"; i=$((i+1)); done
    PIDS="$PIDS $subs"; sleep 0.3
    bench "$port" 16 20000 -r 10 publish fan:__rand_int__ $V$V$V
    bench "$port" 1 3000 -r 10 publish fan:__rand_int__ $V$V$V
    kill $subs 2>/dev/null
    # client side caching in broadcast mode: writes with listeners tracking a
    # key prefix, some of them restricted by ACL key patterns
    cli "$port" acl setuser pgobcast on ">pgopass" "~nomatch:*" "+@all" >/dev/null
    subs=""; i=0; while [ $i -lt 10 ]; do
        { echo "hello 3"; echo "client tracking on bcast prefix key:"; sleep 3; } | "$CLI" -p "$port" >/dev/null 2>&1 & subs="$subs $!"
        { echo "hello 3 auth pgobcast pgopass"; echo "client tracking on bcast prefix key:"; sleep 3; } | "$CLI" -p "$port" >/dev/null 2>&1 & subs="$subs $!"
        i=$((i+1)); done
    PIDS="$PIDS $subs"; sleep 0.3
    bench "$port" 1 4000 set key:__rand_int__ $S
    bench "$port" 16 20000 set key:__rand_int__ $S
    kill $subs 2>/dev/null; wait $subs 2>/dev/null
    cli "$port" acl deluser pgobcast >/dev/null
    # many members / fields in one command: ZMSCORE on a listpack encoded
    # set (hits and misses), HSETEX with 50 big fields
    local zm="" zq="" hf=""; i=1
    while [ $i -le 100 ]; do zm="$zm $i mem$i"; i=$((i+1)); done
    i=1; while [ $i -le 25 ]; do zq="$zq mem$i miss$i"; i=$((i+1)); done
    i=1; while [ $i -le 50 ]; do hf="$hf field:$i $LONG"; i=$((i+1)); done
    bench "$port" 16 2000 -r 500 zadd zm:__rand_int__ $zm
    bench "$port" 16 5000 -r 500 zmscore zm:__rand_int__ $zq
    bench "$port" 1 500 -r 500 zmscore zm:__rand_int__ $zq
    bench "$port" 4 400 -r 200 hsetex hx:__rand_int__ ex 100000 fields 50 $hf
    # RESP3
    bench "$port" 16 10000 -3 get key:__rand_int__
    bench "$port" 16 10000 -3 hgetall sh:__rand_int__
    bench "$port" 16 10000 -3 zrange bz:1 0 9 withscores
    bench "$port" 16 10000 -3 smembers ss:__rand_int__
    # short lived connections and monitoring style commands
    rep 30 "ping" | while read -r c; do cli "$port" $c >/dev/null; done
    rep 20 "info" "dbsize" "client list" "config get maxmemory" "slowlog get 10" "latency latest" "memory stats" | script "$port"
    return 0
}

# --------------------------------------------------------------- standalone
start m $P_M || fail "server did not start"
cli $P_M xgroup create bst:1 grp 0 mkstream >/dev/null
commands $P_M "$CMDS$CMDS_STANDALONE" 100 || fail "command workload failed"
phase "standalone commands"
strings $P_M || fail "string workload failed"
extras $P_M || fail "extra workload failed"
phase "strings+extras"
cli $P_M save >/dev/null || fail "SAVE failed"
cli $P_M bgsave >/dev/null; sleep 0.2
wait_for 600 bgsave_done $P_M || fail "BGSAVE did not finish"
stop m $P_M || fail "server did not stop"

# Restart: loads the RDB written above (all types and encodings). This time
# with io-threads.
start m $P_M --io-threads 4 || fail "server did not restart"
phase "save+restart"
commands $P_M "$CMDS$CMDS_STANDALONE" 100 2 || fail "command workload failed (io-threads)"
strings $P_M || fail "string workload failed (io-threads)"

# Full sync to two replicas (disk-backed and diskless load), a partial resync,
# then a write stream that the replicas have to apply.
start r1 $P_R1 || fail "replica did not start"
phase "io-threads commands"
start r2 $P_R2 --repl-diskless-load flushdb || fail "replica did not start"
cli $P_R1 replicaof 127.0.0.1 $P_M >/dev/null
cli $P_R2 replicaof 127.0.0.1 $P_M >/dev/null
wait_for 1200 link_up $P_R1 || fail "replica sync failed"
wait_for 1200 link_up $P_R2 || fail "replica sync failed"
commands $P_M "$CMDS$CMDS_STANDALONE" 50 2 || fail "command workload failed (replication)"
cli $P_R1 client kill type master >/dev/null; sleep 0.3
wait_for 600 link_up $P_R1
strings $P_M || fail "string workload failed (replication)"
extras $P_M
commands $P_R1 "$CMDS" 20 1                      # reads (and refused writes) on a replica
cli $P_M wait 2 1000 >/dev/null
wait_for 600 caught_up $P_M $P_R1
wait_for 600 caught_up $P_M $P_R2
stop r1 $P_R1; stop r2 $P_R2
stop m $P_M || fail "server did not stop"
phase "replication"

# ------------------------------------------------- eviction (best effort)
eviction_part() {
    start e $P_E --maxmemory 40mb --maxmemory-policy allkeys-lru || return 1
    bench $P_E 16 2000 -r 2000 set blessed:__rand_int__ $V
    bench $P_E 16 2000 -r 2000 bless set blessed:__rand_int__ no-evict
    bench $P_E 16 60000 -r 200000 -d 1024 -t set,get
    commands $P_E "$CMDS" 30 1
    bench $P_E 16 500 bless get blessed:__rand_int__; cli $P_E bless scan 0 no-evict count 100 >/dev/null
    cli $P_E config set maxmemory-policy allkeys-lfu >/dev/null
    bench $P_E 16 60000 -r 200000 -d 1024 -t set,get
    cli $P_E config set maxmemory-policy volatile-ttl >/dev/null
    bench $P_E 16 30000 -r 200000 setex ev:__rand_int__ 1000 $V
    return 0
}
eviction_part || note "eviction workload skipped or incomplete"
stop e $P_E

phase "eviction"
# ------------------------------------------------------ AOF (best effort)
aof_part() {
    start a $P_A --appendonly yes --appendfsync everysec || return 1
    commands $P_A "$CMDS$CMDS_STANDALONE" 50 2 || return 1
    strings $P_A
    cli $P_A bgrewriteaof >/dev/null; sleep 0.2
    wait_for 600 rewrite_done $P_A
    commands $P_A "$CMDS" 20 1
    stop a $P_A || return 1
    start a $P_A --appendonly yes --appendfsync everysec || return 1     # AOF load
    return 0
}
aof_part || note "AOF workload skipped or incomplete"
stop a $P_A

phase "aof"
# ---------------------------------------------------- cluster (best effort)
cluster_part() {
    local C="--cluster-enabled yes --cluster-node-timeout 5000"
    # Newer versions refuse an unauthenticated cluster bus unless told otherwise.
    start c1 $P_C1 $C --cluster-bus-port-protected-mode no || start c1 $P_C1 $C || { note "cluster: node did not start"; return 1; }
    cli $P_C1 config get cluster-bus-port-protected-mode | grep -qx no && C="$C --cluster-bus-port-protected-mode no"
    cli $P_C1 cluster addslotsrange 0 16383 >/dev/null
    wait_for 300 cluster_ok $P_C1 || { note "cluster: state not ok"; return 1; }
    commands $P_C1 "$CMDS" 100 || { note "cluster: command workload failed"; return 1; }
    strings $P_C1
    rep 20 "cluster slots" "cluster shards" "cluster nodes" "cluster info" "cluster keyslot key:1" "cluster countkeysinslot 100" "cluster getkeysinslot 100 10" | script $P_C1
    cli $P_C1 save >/dev/null
    stop c1 $P_C1 || return 1
    start c1 $P_C1 $C || { note "cluster: node did not restart"; return 1; }    # cluster-mode RDB load
    wait_for 300 cluster_ok $P_C1 || return 1

    # Atomic slot migration of half of the slots to a second node.
    start c2 $P_C2 $C || { note "cluster: second node did not start"; return 1; }
    cli $P_C2 cluster meet 127.0.0.1 $P_C1 >/dev/null
    wait_for 300 cluster_ok $P_C2 || { note "cluster: second node did not join"; return 1; }
    sleep 2
    if cli $P_C2 cluster migration import 0 8191 >/dev/null; then
        bench $P_C1 16 20000 set key:__rand_int__ $V          # writes during the migration
        wait_for 1200 asm_done $P_C2 || note "cluster: slot migration did not finish"
    else
        note "cluster: slot migration not available"
    fi
    commands $P_C1 "$CMDS" 20 1                     # half of the keys are redirected now

    # Replica of the first node: cluster-mode full sync and write stream.
    start c3 $P_C3 $C || { note "cluster: replica did not start"; return 1; }
    cli $P_C3 cluster meet 127.0.0.1 $P_C1 >/dev/null
    wait_for 300 cluster_ok $P_C3 || { note "cluster: replica did not join"; return 1; }
    sleep 2
    cli $P_C3 cluster replicate "$(cli $P_C1 cluster myid)" >/dev/null
    wait_for 1200 link_up $P_C3 || { note "cluster: replica sync failed"; return 1; }
    bench $P_C1 16 100000 --cluster -r 200000 -d 64 -t set,get
    bench $P_C1 16 20000 --cluster -r 20000 -t incr,lpush,rpop,sadd,hset,zadd
    wait_for 300 caught_up $P_C1 $P_C3
    { echo "readonly"; rep 2000 "get key:000000000042" "hget sh:000000000042 f1" "get key:000000000007"; } | script $P_C3
    return 0
}
cluster_part || note "cluster workload skipped or incomplete"
stop c3 $P_C3; stop c2 $P_C2; stop c1 $P_C1

phase "cluster"
echo "pgo-train: done"
exit 0
