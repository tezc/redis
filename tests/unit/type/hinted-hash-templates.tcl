start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "skip:cluster"}} {

    # Helper to check encoding
    proc assert_hashtmpl_encoding {key} {
        set enc [r debug encoding $key]
        assert {$enc eq "hashtmpl-lp" || $enc eq "hashtmpl-ar"}
    }

    proc assert_listpack_encoding {key} {
        set encoding [r debug object $key]
        assert {[string match "*encoding:listpack*" $encoding]}
    }

    proc assert_listpackex_encoding {key} {
        set encoding [r debug object $key]
        assert {[string match "*encoding:listpackex*" $encoding]}
    }

    # Helper to get hash template stats from INFO
    proc get_template_stats {} {
        set info [r info keyspace]
        set templates 0
        set keys 0
        foreach line [split $info "\n"] {
            if {[regexp {^hash_templates:(\d+)} $line -> val]} {
                set templates $val
            }
            if {[regexp {^hash_template_keys:(\d+)} $line -> val]} {
                set keys $val
            }
        }
        return [list $templates $keys]
    }

    # ============================================================
    # HIMPORT PREPARE / SET / DISCARD / DISCARDALL
    # ============================================================

    test {HIMPORT PREPARE creates a template} {
        r himport prepare user name email age
    } {OK}

    test {HIMPORT SET creates template-based hash} {
        # Template fields: name email age (user order)
        # Schema fields: sorted by length-first: age, name, email
        # Values: alice alice@example.com 25 → sorted as: 25 alice alice@example.com
        r himport set user:1 user alice alice@example.com 25
        assert_hashtmpl_encoding user:1
        assert_equal [r hgetall user:1] {age 25 name alice email alice@example.com}
    }

    test {HIMPORT SET with wrong field count fails} {
        catch {r himport set user:2 user bob bob@example.com} err
        assert_match "*value count does not match*" $err
    }

    test {HIMPORT SET with unknown template fails} {
        catch {r himport set user:3 unknown alice alice@example.com 25} err
        assert_match "*no such template*" $err
    }

    test {HIMPORT SET replaces existing string key} {
        r set mykey "string value"
        r himport set mykey user charlie charlie@example.com 30
        assert_hashtmpl_encoding mykey
        assert_equal [r type mykey] {hash}
        assert_equal [r hget mykey name] {charlie}
    }

    test {HIMPORT SET replaces existing regular hash} {
        r hset override:hash1 oldfield oldvalue
        r himport set override:hash1 user dave dave@example.com 40
        assert_hashtmpl_encoding override:hash1
        assert_equal [r hget override:hash1 name] {dave}
        assert_equal [r hget override:hash1 oldfield] {}
    }

    test {HIMPORT SET replaces existing template-based hash} {
        r himport set override:hash2 user eve eve@example.com 25
        assert_hashtmpl_encoding override:hash2
        # Replace with different template
        r himport prepare other_tpl city country
        r himport set override:hash2 other_tpl Paris France
        assert_hashtmpl_encoding override:hash2
        assert_equal [r hget override:hash2 city] {Paris}
        assert_equal [r hget override:hash2 name] {}
    }

    test {HIMPORT DISCARD removes template} {
        r himport prepare temptest f1 f2
        r himport discard temptest
        catch {r himport set key1 temptest v1 v2} err
        assert_match "*no such template*" $err
    }

    test {HIMPORT DISCARDALL removes all templates} {
        r himport prepare t1 f1 f2
        r himport prepare t2 f1 f2 f3
        r himport discardall
        catch {r himport set key1 t1 v1 v2} err
        assert_match "*no such template*" $err
    }

    test {HIMPORT PREPARE replaces existing template with same name} {
        # Create template with 2 fields
        r himport prepare reuse a b
        r himport set reuse:1 reuse val_a val_b
        assert_equal [r hgetall reuse:1] {a val_a b val_b}

        # Replace with template with 3 fields (same name)
        r himport prepare reuse x y z
        r himport set reuse:2 reuse val_x val_y val_z
        assert_equal [r hgetall reuse:2] {x val_x y val_y z val_z}

        # Old template definition should be gone - using with 2 values fails
        catch {r himport set reuse:3 reuse v1 v2} err
        assert_match "*value count does not match*" $err

        # Cleanup
        r himport discard reuse
    }

    # ============================================================
    # HSETC - Internal replication command
    # ============================================================

    test {HSETC creates template-based hash} {
        r hsetc hsetc:test name alice email alice@example.com
        assert_hashtmpl_encoding hsetc:test
        # Length-first sort: name (4) < email (5)
        assert_equal [r hgetall hsetc:test] {name alice email alice@example.com}
    }

    test {HSETC replaces existing key of different type} {
        r set hsetc:replace "string"
        r hsetc hsetc:replace field1 value1 field2 value2
        assert_equal [r type hsetc:replace] {hash}
        assert_hashtmpl_encoding hsetc:replace
    }

    # ============================================================
    # Basic hash operations on template-based hashes
    # ============================================================

    test {HGET on template-based hash} {
        r hsetc basic:test name alice email alice@example.com age 25
        assert_equal [r hget basic:test name] {alice}
        assert_equal [r hget basic:test email] {alice@example.com}
        assert_equal [r hget basic:test age] {25}
        assert_equal [r hget basic:test nonexistent] {}
    }

    test {HGETALL on template-based hash} {
        r hsetc basic:getall f1 v1 f2 v2 f3 v3
        set result [r hgetall basic:getall]
        assert_equal [llength $result] 6
        assert_equal [lindex $result 0] {f1}
        assert_equal [lindex $result 1] {v1}
    }

    test {HLEN on template-based hash} {
        r hsetc basic:len a 1 b 2 c 3 d 4
        assert_equal [r hlen basic:len] 4
    }

    test {HEXISTS on template-based hash} {
        r hsetc basic:exists name alice
        assert_equal [r hexists basic:exists name] 1
        assert_equal [r hexists basic:exists nonexistent] 0
    }

    test {HINCRBY on template-based hash} {
        r hsetc basic:incr counter 10
        assert_equal [r hincrby basic:incr counter 5] 15
        assert_equal [r hget basic:incr counter] 15
        assert_hashtmpl_encoding basic:incr
    }

    test {HINCRBYFLOAT on template-based hash} {
        r hsetc basic:incrfloat value 10.5
        set result [r hincrbyfloat basic:incrfloat value 0.1]
        assert {$result >= 10.5 && $result <= 10.7}
        assert_hashtmpl_encoding basic:incrfloat
    }

    test {HMGET on template-based hash} {
        r hsetc basic:hmget a 1 b 2 c 3
        assert_equal [r hmget basic:hmget a c] {1 3}
        assert_equal [r hmget basic:hmget a nonexistent c] {1 {} 3}
    }

    test {HKEYS on template-based hash} {
        r hsetc basic:keys name alice email bob
        set keys [r hkeys basic:keys]
        assert_equal [lsort $keys] {email name}
    }

    test {HVALS on template-based hash} {
        r hsetc basic:vals name alice email bob
        set vals [r hvals basic:vals]
        assert_equal [lsort $vals] {alice bob}
    }

    test {HSTRLEN on template-based hash} {
        r hsetc basic:strlen name alice
        assert_equal [r hstrlen basic:strlen name] 5
    }

    # ============================================================
    # HDEL - Field deletion (stays template-based with new template)
    # ============================================================

    test {HDEL on template-based hash keeps template encoding} {
        r hsetc hdel:test a 1 b 2 c 3 d 4
        assert_hashtmpl_encoding hdel:test
        assert_equal [r hdel hdel:test b] 1
        assert_hashtmpl_encoding hdel:test
        assert_equal [r hlen hdel:test] 3
        assert_equal [r hexists hdel:test b] 0
        assert_equal [r hget hdel:test a] 1
    }

    test {HDEL multiple fields on template-based hash} {
        r hsetc hdel:multi a 1 b 2 c 3 d 4 e 5
        assert_equal [r hdel hdel:multi b d] 2
        assert_hashtmpl_encoding hdel:multi
        assert_equal [r hlen hdel:multi] 3
    }

    test {HDEL all fields deletes the key} {
        r hsetc hdel:all a 1 b 2
        r hdel hdel:all a b
        assert_equal [r exists hdel:all] 0
    }

    # ============================================================
    # HSET - Adding new fields (creates new template)
    # ============================================================

    test {HSET adds new field to template-based hash} {
        r hsetc hset:add name alice email alice@example.com
        assert_hashtmpl_encoding hset:add
        r hset hset:add age 25
        assert_hashtmpl_encoding hset:add
        assert_equal [r hlen hset:add] 3
        assert_equal [r hget hset:add age] 25
    }

    test {HSET updates existing field in template-based hash} {
        r hsetc hset:update name alice
        r hset hset:update name bob
        assert_hashtmpl_encoding hset:update
        assert_equal [r hget hset:update name] bob
    }

    test {HSET multiple fields on template-based hash} {
        r hsetc hset:multi a 1
        r hset hset:multi b 2 c 3 d 4
        assert_hashtmpl_encoding hset:multi
        assert_equal [r hlen hset:multi] 4
    }

    # ============================================================
    # HSCAN on template-based hash
    # ============================================================

    test {HSCAN on template-based hash returns all fields} {
        r hsetc hscan:test a 1 b 2 c 3
        set result [r hscan hscan:test 0]
        set cursor [lindex $result 0]
        set elements [lindex $result 1]
        assert_equal $cursor 0
        assert_equal [llength $elements] 6
    }

    test {HSCAN with MATCH pattern on template-based hash} {
        r hsetc hscan:match field1 val1 field2 val2 other val3
        set result [r hscan hscan:match 0 MATCH field*]
        set elements [lindex $result 1]
        # Should match field1 and field2
        assert {[llength $elements] == 4}
    }

    test {HSCAN with NOVALUES on template-based hash} {
        r hsetc hscan:noval a 1 b 2 c 3
        set result [r hscan hscan:noval 0 NOVALUES]
        set elements [lindex $result 1]
        assert_equal [llength $elements] 3
        assert_equal [lsort $elements] {a b c}
    }

    # ============================================================
    # HRANDFIELD on template-based hash
    # ============================================================

    test {HRANDFIELD on template-based hash} {
        r hsetc hrand:test a 1 b 2 c 3
        set field [r hrandfield hrand:test]
        assert {$field eq "a" || $field eq "b" || $field eq "c"}
    }

    test {HRANDFIELD with count on template-based hash} {
        r hsetc hrand:count a 1 b 2 c 3 d 4
        set fields [r hrandfield hrand:count 2]
        assert_equal [llength $fields] 2
    }

    test {HRANDFIELD with WITHVALUES on template-based hash} {
        r hsetc hrand:withval a 1 b 2
        set result [r hrandfield hrand:withval 2 WITHVALUES]
        assert_equal [llength $result] 4
    }

    test {HRANDFIELD negative count on template-based hash} {
        r hsetc hrand:neg a 1 b 2
        set result [r hrandfield hrand:neg -5]
        assert_equal [llength $result] 5
    }

    # ============================================================
    # Hash Field Expiration - converts to listpackex
    # ============================================================

    test {HPEXPIRE on template-based hash converts to listpackex} {
        r hsetc hfe:test name alice email alice@example.com
        assert_hashtmpl_encoding hfe:test
        r hpexpire hfe:test 100000 FIELDS 1 name
        assert_listpackex_encoding hfe:test
        assert_equal [r hget hfe:test name] alice
        assert_equal [r hget hfe:test email] alice@example.com
    }

    test {HEXPIRE on template-based hash converts to listpackex} {
        r hsetc hfe:expire name bob age 30
        assert_hashtmpl_encoding hfe:expire
        r hexpire hfe:expire 100 FIELDS 1 age
        assert_listpackex_encoding hfe:expire
    }

    test {HSETEX without expiration keeps template encoding} {
        r hsetc hfe:noexp name alice
        # HSET without expiration should keep template-based encoding
        r hset hfe:noexp email alice@example.com
        assert_hashtmpl_encoding hfe:noexp
    }

    # ============================================================
    # DUMP/RESTORE on template-based hash
    # ============================================================

    test {DUMP/RESTORE preserves template-based hash} {
        r hsetc dump:test name alice email alice@example.com age 25
        assert_hashtmpl_encoding dump:test
        set dump [r dump dump:test]
        r del dump:test
        r restore dump:test 0 $dump
        assert_hashtmpl_encoding dump:test
        # Length-first sort: age (3) < name (4) < email (5)
        assert_equal [r hgetall dump:test] {age 25 name alice email alice@example.com}
    }

    test {DUMP/RESTORE to different key} {
        r hsetc dump:src a 1 b 2 c 3
        set dump [r dump dump:src]
        r restore dump:dst 0 $dump
        assert_hashtmpl_encoding dump:dst
        assert_equal [r hgetall dump:dst] {a 1 b 2 c 3}
    }

    # ============================================================
    # Schema sharing between multiple keys
    # ============================================================

    test {Multiple keys share the same template} {
        r himport prepare shared name email age
        r himport set shared:1 shared alice alice@example.com 25
        r himport set shared:2 shared bob bob@example.com 30
        r himport set shared:3 shared charlie charlie@example.com 35

        assert_hashtmpl_encoding shared:1
        assert_hashtmpl_encoding shared:2
        assert_hashtmpl_encoding shared:3

        assert_equal [r hget shared:1 name] alice
        assert_equal [r hget shared:2 name] bob
        assert_equal [r hget shared:3 name] charlie
    }

    test {HSETC creates shared template for same field set} {
        r hsetc tplshare:1 f1 v1 f2 v2
        r hsetc tplshare:2 f1 v3 f2 v4
        assert_hashtmpl_encoding tplshare:1
        assert_hashtmpl_encoding tplshare:2
    }

    test {Different field orders use same template} {
        # Templates with same fields in different order should share template
        r himport prepare order1 a b c
        r himport prepare order2 c b a
        r himport prepare order3 b a c

        # Create hashes - values follow template field order
        r himport set order:key1 order1 va1 vb1 vc1
        r himport set order:key2 order2 vc2 vb2 va2
        r himport set order:key3 order3 vb3 va3 vc3

        assert_hashtmpl_encoding order:key1
        assert_hashtmpl_encoding order:key2
        assert_hashtmpl_encoding order:key3

        # All should have same sorted field order: a, b, c
        assert_equal [r hget order:key1 a] va1
        assert_equal [r hget order:key2 a] va2
        assert_equal [r hget order:key3 a] va3

        assert_equal [r hget order:key1 b] vb1
        assert_equal [r hget order:key2 b] vb2
        assert_equal [r hget order:key3 b] vb3
    }

    # ============================================================
    # INFO template stats
    # ============================================================

    test {INFO keyspace shows hash template stats} {
        r flushall
        lassign [get_template_stats] tpl_before keys_before
        assert_equal $keys_before 0

        # Create 3 keys with same template
        r hsetc info:k1 a 1 b 2
        r hsetc info:k2 a 3 b 4
        r hsetc info:k3 a 5 b 6
        lassign [get_template_stats] tpl1 keys1
        assert_equal $keys1 3

        # Create 1 key with different template
        r hsetc info:k4 x 1 y 2 z 3
        lassign [get_template_stats] tpl2 keys2
        assert_equal $keys2 4

        # Delete one key
        r del info:k1
        lassign [get_template_stats] tpl3 keys3
        assert_equal $keys3 3

        # Flushall resets to 0
        r flushall
        lassign [get_template_stats] tpl_after keys_after
        assert_equal $keys_after 0
    }

    # ============================================================
    # FLUSHALL / FLUSHDB - async free safety
    # ============================================================

    test {FLUSHALL with template-based hashes does not crash} {
        r hsetc flush:test1 a 1 b 2 c 3
        r hsetc flush:test2 a 4 b 5 c 6
        r hsetc flush:test3 x 1 y 2
        assert_hashtmpl_encoding flush:test1
        r flushall
        assert_equal [r dbsize] 0
        # Create new hashes after flush
        r hsetc flush:new a 1 b 2
        assert_hashtmpl_encoding flush:new
        assert_equal [r hget flush:new a] 1
    }

    test {FLUSHDB with template-based hashes does not crash} {
        r select 1
        r hsetc flushdb:test a 1 b 2
        assert_hashtmpl_encoding flushdb:test
        r flushdb
        assert_equal [r dbsize] 0
        r select 0
    }

    test {Multiple FLUSHALL in succession does not crash} {
        r hsetc multi:flush a 1 b 2
        r flushall
        r flushall
        r flushall
        r ping
    } {PONG}

    # ============================================================
    # DEL - template refcount management
    # ============================================================

    test {DEL on template-based hash releases template ref} {
        r hsetc del:test name alice email bob
        assert_hashtmpl_encoding del:test
        r del del:test
        assert_equal [r exists del:test] 0
        # Should be able to create new hash with same template
        r hsetc del:new name charlie email dave
        assert_hashtmpl_encoding del:new
    }

    # ============================================================
    # HSETNX on template-based hash
    # ============================================================

    test {HSETNX on existing field in template-based hash} {
        r hsetc hsetnx:test name alice
        assert_equal [r hsetnx hsetnx:test name bob] 0
        assert_equal [r hget hsetnx:test name] alice
    }

    test {HSETNX on new field in template-based hash} {
        r hsetc hsetnx:new name alice
        assert_equal [r hsetnx hsetnx:new email alice@example.com] 1
        assert_hashtmpl_encoding hsetnx:new
        assert_equal [r hget hsetnx:new email] alice@example.com
    }

    # ============================================================
    # Key-level expiration on template-based hash
    # ============================================================

    test {EXPIRE on template-based hash works} {
        r hsetc expire:test name alice
        r expire expire:test 100
        set ttl [r ttl expire:test]
        assert {$ttl > 0 && $ttl <= 100}
        assert_hashtmpl_encoding expire:test
    }

    # ============================================================
    # COPY command on template-based hash
    # ============================================================

    test {COPY template-based hash to new key} {
        r hsetc copy:src name alice email bob
        assert_hashtmpl_encoding copy:src
        r copy copy:src copy:dst
        assert_hashtmpl_encoding copy:dst
        assert_equal [r hgetall copy:dst] [r hgetall copy:src]
    }

    test {COPY template-based hash with REPLACE} {
        r hsetc copy:replace:src a 1 b 2
        r set copy:replace:dst "string"
        r copy copy:replace:src copy:replace:dst REPLACE
        assert_equal [r type copy:replace:dst] hash
        assert_hashtmpl_encoding copy:replace:dst
    }

}

# ============================================================
# RDB SAVE/LOAD tests (require server restart)
# ============================================================

start_server {tags {"hash" "hinted-hash-templates" "rdb" "needs:debug"}} {

    proc assert_hashtmpl_encoding {key} {
        set enc [r debug encoding $key]
        assert {$enc eq "hashtmpl-lp" || $enc eq "hashtmpl-ar"}
    }

    test {RDB save and load preserves template-based hash} {
        r config set rdb-load-hash-template-threshold-fields 1

        r hsetc rdb:test name alice email alice@example.com age 25
        assert_hashtmpl_encoding rdb:test

        r debug reload

        assert_hashtmpl_encoding rdb:test
        # Length-first sort: age (3) < name (4) < email (5)
        assert_equal [r hgetall rdb:test] {age 25 name alice email alice@example.com}
    }

    test {RDB save and load multiple template-based hashes with shared template} {
        r config set rdb-load-hash-template-threshold-fields 1

        r flushall
        r hsetc rdb:multi1 a 1 b 2 c 3
        r hsetc rdb:multi2 a 4 b 5 c 6
        r hsetc rdb:multi3 x 1 y 2

        r debug reload

        assert_hashtmpl_encoding rdb:multi1
        assert_hashtmpl_encoding rdb:multi2
        assert_hashtmpl_encoding rdb:multi3
        assert_equal [r hget rdb:multi1 a] 1
        assert_equal [r hget rdb:multi2 b] 5
    }



}

# ============================================================
# Replication tests
# ============================================================

start_server {tags {"hash" "hinted-hash-templates" "repl" "needs:repl" "needs:debug"}} {
    start_server {} {
        test {HIMPORT SET replicates as HSETC} {
            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set replica [srv 0 client]

            $replica replicaof $master_host $master_port
            wait_for_condition 50 100 {
                [s 0 master_link_status] eq "up"
            } else {
                fail "Replica did not sync"
            }

            # Template: name email (user order) → Schema: email name (sorted)
            $master himport prepare user name email
            $master himport set repl:test user alice alice@example.com

            wait_for_condition 50 100 {
                [$replica exists repl:test] == 1
            } else {
                fail "Key not replicated"
            }

            # Check encoding on replica
            set master_enc [$master debug encoding repl:test]
            set replica_enc [$replica debug encoding repl:test]

            assert {$master_enc eq "hashtmpl-lp" ||
                    $master_enc eq "hashtmpl-ar"}
            assert {$replica_enc eq "hashtmpl-lp" ||
                    $replica_enc eq "hashtmpl-ar"}
            # Length-first sort: name (4) < email (5)
            assert_equal [$replica hgetall repl:test] {name alice email alice@example.com}
        }
    }
}

