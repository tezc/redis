# Run the full test suite under both template encodings: TMPL_LP (default
# listpack-backed) and TMPL_ARRAY (forced via hash-max-listpack-entries=0).
foreach encoding {template-listpack template-array} {
start_server {tags {"hash" "needs:debug" "cluster:skip"} overrides {hash-min-template-entries 0}} {
    if {$encoding eq "template-array"} {
        r config set hash-max-listpack-entries 0
    }

    # Helper to check encoding. With hash-min-template-entries=0,
    # OBJECT ENCODING reports the real template encoding name.
    proc assert_hashtmpl_encoding {key} {
        set enc [r object encoding $key]
        assert {$enc eq "template-listpack" || $enc eq "template-array"}
    }

    # Build a template-based hash via the user-facing HIMPORT API.
    # HSETC is internal (CMD_INTERNAL) and only accepted from master/AOF.
    proc make_hashtmpl {key args} {
        set fields {}
        set values {}
        foreach {f v} $args {
            lappend fields $f
            lappend values $v
        }
        set tplname "tpl_[join $fields _]"
        r himport prepare $tplname {*}$fields
        r himport set $key $tplname {*}$values
    }

    # Helper to get hash template stats from INFO
    proc get_template_stats {} {
        set info [r info stats]
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

    # A key-ref released by a BIO lazyfree thread (flushall, resync, etc.) is
    # dropped on that background thread, so hash_template_keys is eventually
    # consistent: it settles once the BIO free job runs. Poll for the expected
    # value on the given client ('r' by default, or a level such as 0 / -1 for
    # replication tests).
    proc wait_hashtmpl_keys {expected {level ""}} {
        wait_for_condition 50 100 {
            [s {*}$level hash_template_keys] == $expected
        } else {
            fail "hash_template_keys did not settle to $expected\
                  (got [s {*}$level hash_template_keys])"
        }
    }

    # Poll hash_templates (registry size) until it settles. A template is removed
    # only once both its key-refs and hold-refs reach zero; the final key-ref may
    # be dropped on a BIO lazyfree thread and reclaimed in serverCron, so the
    # registry size is eventually consistent like hash_template_keys above.
    proc wait_hashtmpl_templates {expected {level ""}} {
        wait_for_condition 50 100 {
            [s {*}$level hash_templates] == $expected
        } else {
            fail "hash_templates did not settle to $expected\
                  (got [s {*}$level hash_templates])"
        }
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

    test {HIMPORT SET with unknown fieldset fails} {
        catch {r himport set user:3 unknown alice alice@example.com 25} err
        assert_match "*no such fieldset*" $err
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

    test {HIMPORT DISCARD removes fieldset} {
        r himport prepare temptest f1 f2
        r himport discard temptest
        catch {r himport set key1 temptest v1 v2} err
        assert_match "*no such fieldset*" $err
    }

    test {HIMPORT DISCARDALL removes all fieldsets} {
        r himport prepare t1 f1 f2
        r himport prepare t2 f1 f2 f3
        r himport discardall
        catch {r himport set key1 t1 v1 v2} err
        assert_match "*no such fieldset*" $err
    }

    test {HIMPORT PREPARE state is accounted in client memory} {
        # tot-mem from CLIENT INFO for the current connection.
        proc cur_tot_mem {} {
            regexp {tot-mem=(\d+)} [r client info] -> m
            return $m
        }

        r himport discardall
        set before [cur_tot_mem]

        # Prepare many large fieldsets; each pins a template and owns a
        # value_order map plus the fieldset name.
        for {set i 0} {$i < 100} {incr i} {
            set fields {}
            for {set f 0} {$f < 64} {incr f} {
                lappend fields "field_${i}_${f}"
            }
            r himport prepare fs$i {*}$fields
        }
        set after [cur_tot_mem]

        # Client-owned fieldset state must be visible in tot-mem.
        assert {$after > $before}

        # Releasing the fieldsets returns the accounted memory.
        r himport discardall
        set discarded [cur_tot_mem]
        assert {$discarded < $after}
    }

    test {HIMPORT PREPARE state can trigger maxmemory-clients eviction} {
        # Returns the CLIENT LIST entry for $name, or "" if not connected.
        proc himport_client_line {name} {
            set clients [split [string trim [r client list]] "\r\n"]
            return [lsearch -inline $clients *name=$name*]
        }

        set saved_limit [lindex [r config get maxmemory-clients] 1]
        r config set maxmemory-clients 2mb
        r client no-evict on ;# protect the main test connection

        # A separate connection that accumulates HIMPORT fieldset state. A
        # single PREPARE's query buffer (~tens of KB) stays far below the 2mb
        # limit, so eviction can only fire once the accounted fieldset memory
        # (value_order maps) adds up past it.
        set rr [redis_client]
        $rr client setname himport_abuser
        assert {[himport_client_line himport_abuser] ne ""}

        set evicted 0
        for {set i 0} {$i < 4000} {incr i} {
            set fields {}
            for {set f 0} {$f < 2000} {incr f} {
                lappend fields "field_${i}_${f}"
            }
            if {[catch {$rr himport prepare fs$i {*}$fields}]} {
                set evicted 1 ;# server closed the connection on eviction
                break
            }
            if {[himport_client_line himport_abuser] eq ""} {
                set evicted 1 ;# evicted asynchronously in beforeSleep
                break
            }
        }
        assert {$evicted}

        catch {$rr close}
        r config set maxmemory-clients $saved_limit
    }

    test {HIMPORT PREPARE replaces existing fieldset with same name} {
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
    # Argument validation
    # ============================================================

    test {HIMPORT with no subcommand returns arity error} {
        catch {r himport} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT with unknown subcommand returns error} {
        catch {r himport foobar} err
        assert_match "*unknown subcommand*" $err
    }

    test {HIMPORT subcommand is case-insensitive} {
        assert_equal [r himport PREPARE caseT a b] OK
        assert_equal [r himport Prepare caseT2 a b] OK
        assert_equal [r HIMPORT prepare caseT3 a b] OK
        r himport discardall
    }

    # --- HIMPORT PREPARE arity / validation ---

    test {HIMPORT PREPARE with no fieldset name fails} {
        catch {r himport prepare} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT PREPARE with no fields fails} {
        catch {r himport prepare foo} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT PREPARE with single field works} {
        assert_equal [r himport prepare single f1] OK
        assert_equal [r himport set single:k single v1] OK
        assert_equal [r hgetall single:k] {f1 v1}
        r himport discard single
    }

    test {HIMPORT PREPARE rejects duplicate field names} {
        catch {r himport prepare dup1 a a b} err
        assert_match "*duplicate field name*" $err
        catch {r himport prepare dup2 a b a} err
        assert_match "*duplicate field name*" $err
        catch {r himport prepare dup3 same same} err
        assert_match "*duplicate field name*" $err
        # Failed PREPARE must not register the fieldset.
        catch {r himport set dup:k dup1 v1 v2 v3} err
        assert_match "*no such fieldset*" $err
    }

    test {HIMPORT PREPARE accepts empty field name} {
        assert_equal [r himport prepare emptyf "" b] OK
        assert_equal [r himport set emptyf:k emptyf va vb] OK
        assert_equal [r hget emptyf:k ""] va
        assert_equal [r hget emptyf:k b] vb
        r himport discard emptyf
    }

    test {HIMPORT PREPARE accepts empty template name} {
        assert_equal [r himport prepare "" f1 f2] OK
        assert_equal [r himport set emptynam:k "" v1 v2] OK
        assert_equal [r hgetall emptynam:k] {f1 v1 f2 v2}
        r himport discard ""
    }

    # --- HIMPORT SET arity / validation ---

    test {HIMPORT SET with no key fails} {
        catch {r himport set} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT SET with no template fails} {
        catch {r himport set k} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT SET with no values fails} {
        catch {r himport set k tpl} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT SET with too many values fails} {
        r himport prepare twof a b
        catch {r himport set twof:k twof v1 v2 v3} err
        assert_match "*value count does not match*" $err
        r himport discard twof
    }

    test {HIMPORT SET with too few values fails} {
        r himport prepare threef a b c
        catch {r himport set threef:k threef v1 v2} err
        assert_match "*value count does not match*" $err
        r himport discard threef
    }

    # --- HIMPORT DISCARD arity / behavior ---

    test {HIMPORT DISCARD with no fieldset name fails} {
        catch {r himport discard} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT DISCARD with extra args fails} {
        catch {r himport discard a b} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT DISCARD on nonexistent fieldset returns 0} {
        assert_equal [r himport discard does_not_exist] 0
    }

    test {HIMPORT DISCARD returns 1 when fieldset removed} {
        r himport prepare disc1 a b
        assert_equal [r himport discard disc1] 1
        assert_equal [r himport discard disc1] 0
    }

    test {HIMPORT DISCARD does not invalidate existing keys} {
        r himport prepare ref a b c
        r himport set ref:k1 ref v1 v2 v3
        assert_hashtmpl_encoding ref:k1
        # Discard the fieldset; existing key must remain valid.
        r himport discard ref
        assert_hashtmpl_encoding ref:k1
        assert_equal [r hgetall ref:k1] {a v1 b v2 c v3}
        # SET with the discarded name now fails.
        catch {r himport set ref:k2 ref v1 v2 v3} err
        assert_match "*no such fieldset*" $err
        r del ref:k1
    }

    # --- HIMPORT DISCARDALL arity / behavior ---

    test {HIMPORT DISCARDALL with extra args fails} {
        catch {r himport discardall extra} err
        assert_match "*wrong number of arguments*" $err
    }

    test {HIMPORT DISCARDALL with no fieldsets returns 0} {
        r himport discardall
        assert_equal [r himport discardall] 0
    }

    test {HIMPORT DISCARDALL returns count of removed fieldsets} {
        r himport discardall
        r himport prepare a1 x y
        r himport prepare a2 x y z
        r himport prepare a3 m n
        assert_equal [r himport discardall] 3
        foreach name {a1 a2 a3} {
            catch {r himport set k $name v1 v2} err
            assert_match "*no such fieldset*" $err
        }
    }

    # --- Session isolation ---

    test {Fieldsets are not shared between connections} {
        set rd [redis_client]
        $rd himport prepare iso a b
        # Main connection cannot see the other connection's fieldset.
        catch {r himport set iso:k iso v1 v2} err
        assert_match "*no such fieldset*" $err
        # The fieldset still works on its owning connection.
        assert_equal [$rd himport set iso:k iso v1 v2] OK
        $rd close
        r del iso:k
    }

    test {CLIENT RESET clears session-local fieldsets} {
        r himport prepare resettpl a b c
        assert_equal [r himport set reset:k1 resettpl v1 v2 v3] OK
        r reset
        catch {r himport set reset:k2 resettpl v1 v2 v3} err
        assert_match "*no such fieldset*" $err
        r del reset:k1
    }

    test {EVAL invocations do not share session-local fieldsets} {
        assert_equal [r eval {redis.call('HIMPORT','PREPARE','evaltpl','a'); return 'OK'} 0] OK

        catch {r eval {return redis.call('HIMPORT','SET','eval:k','evaltpl','1')} 0} err
        assert_match "*no such fieldset*" $err

        assert_equal [r eval {
            redis.call('HIMPORT','PREPARE','evaltpl','a')
            return redis.call('HIMPORT','SET','eval:k','evaltpl','1')
        } 0] OK
        assert_equal [r hgetall eval:k] {a 1}
        r del eval:k
    }

    test {FCALL invocations do not share session-local fieldsets} {
        r function flush
        r function load replace {#!lua name=himporttest
            redis.register_function('prepare_only', function(KEYS, ARGV)
                return redis.call('HIMPORT','PREPARE','functpl','a')
            end)
            redis.register_function('set_only', function(KEYS, ARGV)
                return redis.call('HIMPORT','SET',KEYS[1],'functpl','1')
            end)
            redis.register_function('prepare_and_set', function(KEYS, ARGV)
                redis.call('HIMPORT','PREPARE','functpl','a')
                return redis.call('HIMPORT','SET',KEYS[1],'functpl','1')
            end)
        }

        assert_equal [r fcall prepare_only 0] OK
        catch {r fcall set_only 1 func:k} err
        assert_match "*no such fieldset*" $err

        assert_equal [r fcall prepare_and_set 1 func:k] OK
        assert_equal [r hgetall func:k] {a 1}
        r del func:k
        r function flush
    }

    # --- MULTI / EXEC ---

    test {HIMPORT PREPARE/SET inside MULTI/EXEC works} {
        r multi
        r himport prepare mtpl a b c
        r himport set multi:k mtpl v1 v2 v3
        set replies [r exec]
        assert_equal [lindex $replies 0] OK
        assert_equal [lindex $replies 1] OK
        assert_hashtmpl_encoding multi:k
        assert_equal [r hgetall multi:k] {a v1 b v2 c v3}
        r himport discard mtpl
    }

    test {HIMPORT DISCARDALL inside MULTI/EXEC works} {
        r himport prepare mdt1 a b
        r himport prepare mdt2 c d
        r multi
        r himport discardall
        r exec
        catch {r himport set k mdt1 v1 v2} err
        assert_match "*no such fieldset*" $err
    }


    # ============================================================
    # HSETC - Internal replication command (rejected from user clients)
    # ============================================================

    test {HSETC is rejected from non-internal clients} {
        catch {r hsetc hsetc:test name alice} err
        assert_match "*unknown command*" $err
    }

    test {HSETC is rejected from scripts (EVAL/FCALL)} {
        catch {r eval {return redis.call('HSETC', KEYS[1], 'a', 'va')} 1 hsetc:eval} e1
        assert_match "*not allowed from script*" $e1
        r function load replace {#!lua name=hsetclib
            redis.register_function('hsetc_call', function(keys, args)
                return redis.call('HSETC', keys[1], 'a', 'va')
            end)}
        catch {r fcall hsetc_call 1 hsetc:fcall} e2
        assert_match "*not allowed from script*" $e2
        # No corrupt template/key created via either path.
        assert_equal 0 [r exists hsetc:eval]
        assert_equal 0 [r exists hsetc:fcall]
    }

    # ============================================================
    # Basic hash operations on template-based hashes
    # ============================================================

    test {HGET on template-based hash} {
        make_hashtmpl basic:test name alice email alice@example.com age 25
        assert_equal [r hget basic:test name] {alice}
        assert_equal [r hget basic:test email] {alice@example.com}
        assert_equal [r hget basic:test age] {25}
        assert_equal [r hget basic:test nonexistent] {}
    }

    test {HGETALL on template-based hash} {
        make_hashtmpl basic:getall f1 v1 f2 v2 f3 v3
        set result [r hgetall basic:getall]
        assert_equal [llength $result] 6
        assert_equal [lindex $result 0] {f1}
        assert_equal [lindex $result 1] {v1}
    }

    test {HLEN on template-based hash} {
        make_hashtmpl basic:len a 1 b 2 c 3 d 4
        assert_equal [r hlen basic:len] 4
    }

    test {HEXISTS on template-based hash} {
        make_hashtmpl basic:exists name alice
        assert_equal [r hexists basic:exists name] 1
        assert_equal [r hexists basic:exists nonexistent] 0
    }

    test {HINCRBY on template-based hash} {
        make_hashtmpl basic:incr counter 10
        assert_equal [r hincrby basic:incr counter 5] 15
        assert_equal [r hget basic:incr counter] 15
        assert_hashtmpl_encoding basic:incr
    }

    test {HINCRBYFLOAT on template-based hash} {
        make_hashtmpl basic:incrfloat value 10.5
        set result [r hincrbyfloat basic:incrfloat value 0.1]
        assert {$result >= 10.5 && $result <= 10.7}
        assert_hashtmpl_encoding basic:incrfloat
    }

    test {HMGET on template-based hash} {
        make_hashtmpl basic:hmget a 1 b 2 c 3
        assert_equal [r hmget basic:hmget a c] {1 3}
        assert_equal [r hmget basic:hmget a nonexistent c] {1 {} 3}
    }

    test {HKEYS on template-based hash} {
        make_hashtmpl basic:keys name alice email bob
        set keys [r hkeys basic:keys]
        assert_equal [lsort $keys] {email name}
    }

    test {HVALS on template-based hash} {
        make_hashtmpl basic:vals name alice email bob
        set vals [r hvals basic:vals]
        assert_equal [lsort $vals] {alice bob}
    }

    test {HSTRLEN on template-based hash} {
        make_hashtmpl basic:strlen name alice
        assert_equal [r hstrlen basic:strlen name] 5
    }

    # ============================================================
    # HDEL - Field deletion (stays template-based with new template)
    # ============================================================

    test {HDEL on template-based hash keeps template encoding} {
        make_hashtmpl hdel:test a 1 b 2 c 3 d 4
        assert_hashtmpl_encoding hdel:test
        assert_equal [r hdel hdel:test b] 1
        assert_hashtmpl_encoding hdel:test
        assert_equal [r hlen hdel:test] 3
        assert_equal [r hexists hdel:test b] 0
        assert_equal [r hget hdel:test a] 1
    }

    test {HDEL multiple fields on template-based hash} {
        make_hashtmpl hdel:multi a 1 b 2 c 3 d 4 e 5
        assert_equal [r hdel hdel:multi b d] 2
        assert_hashtmpl_encoding hdel:multi
        assert_equal [r hlen hdel:multi] 3
    }

    test {HDEL all fields deletes the key} {
        make_hashtmpl hdel:all a 1 b 2
        r hdel hdel:all a b
        assert_equal [r exists hdel:all] 0
    }

    # ============================================================
    # HGETDEL - Get and delete fields (template-aware delete path)
    # ============================================================

    test {HGETDEL returns value and keeps template encoding} {
        make_hashtmpl hgetdel:test a 1 b 2 c 3 d 4
        assert_hashtmpl_encoding hgetdel:test
        assert_equal [r hgetdel hgetdel:test FIELDS 1 b] {2}
        assert_hashtmpl_encoding hgetdel:test
        assert_equal [r hlen hgetdel:test] 3
        assert_equal [r hexists hgetdel:test b] 0
        assert_equal [r hget hgetdel:test a] 1
    }

    test {HGETDEL multiple fields on template-based hash} {
        make_hashtmpl hgetdel:multi a 1 b 2 c 3 d 4 e 5
        assert_equal [r hgetdel hgetdel:multi FIELDS 2 b d] {2 4}
        assert_hashtmpl_encoding hgetdel:multi
        assert_equal [r hlen hgetdel:multi] 3
    }

    test {HGETDEL non-existent field returns nil and keeps encoding} {
        make_hashtmpl hgetdel:miss a 1 b 2
        set res [r hgetdel hgetdel:miss FIELDS 1 zzz]
        assert_equal [lindex $res 0] {}
        assert_hashtmpl_encoding hgetdel:miss
        assert_equal [r hlen hgetdel:miss] 2
    }

    test {HGETDEL all fields deletes the key} {
        make_hashtmpl hgetdel:all a 1 b 2
        assert_equal [r hgetdel hgetdel:all FIELDS 2 a b] {1 2}
        assert_equal [r exists hgetdel:all] 0
    }

    # ============================================================
    # HGETEX - Get and optionally set TTL (template-aware path)
    # ============================================================

    test {HGETEX without options returns values and keeps template encoding} {
        make_hashtmpl hgetex:plain a 1 b 2 c 3
        assert_hashtmpl_encoding hgetex:plain
        assert_equal [r hgetex hgetex:plain FIELDS 2 a c] {1 3}
        assert_hashtmpl_encoding hgetex:plain
    }

    test {HGETEX non-existent field returns nil and keeps encoding} {
        make_hashtmpl hgetex:miss a 1 b 2
        set res [r hgetex hgetex:miss FIELDS 1 zzz]
        assert_equal [lindex $res 0] {}
        assert_hashtmpl_encoding hgetex:miss
    }

    test {HGETEX PERSIST converts away from template, no TTL set} {
        make_hashtmpl hgetex:persist a 1 b 2
        assert_hashtmpl_encoding hgetex:persist
        assert_equal [r hgetex hgetex:persist PERSIST FIELDS 1 a] {1}
        # Any expiration flag forces a template hash into an HFE-capable
        # encoding (listpackex or hashtable); it is no longer template-based.
        set enc [r object encoding hgetex:persist]
        assert {$enc ne "template-listpack" && $enc ne "template-array"}
        assert_equal [lindex [r httl hgetex:persist FIELDS 1 a] 0] -1
        assert_equal [r hget hgetex:persist a] 1
    }

    # EX conversion target is listpackex only when it fits the listpack limits;
    # under the TMPL_ARRAY iter (entries=0) it becomes hashtable instead.
    if {$encoding eq "template-listpack"} {
    test {HGETEX EX converts template to listpackex and sets TTL} {
        make_hashtmpl hgetex:ex name alice email alice@example.com
        assert_hashtmpl_encoding hgetex:ex
        assert_equal [r hgetex hgetex:ex EX 100 FIELDS 1 name] {alice}
        assert_match "*encoding:listpackex*" [r debug object hgetex:ex]
        set ttl [lindex [r httl hgetex:ex FIELDS 1 name] 0]
        assert {$ttl > 0 && $ttl <= 100}
        assert_equal [r hget hgetex:ex email] alice@example.com
    }
    } ;# end if template-listpack

    # ============================================================
    # HSET - Adding new fields (creates new template)
    # ============================================================

    test {HSET adds new field to template-based hash} {
        make_hashtmpl hset:add name alice email alice@example.com
        assert_hashtmpl_encoding hset:add
        r hset hset:add age 25
        assert_hashtmpl_encoding hset:add
        assert_equal [r hlen hset:add] 3
        assert_equal [r hget hset:add age] 25
    }

    test {HSET updates existing field in template-based hash} {
        make_hashtmpl hset:update name alice
        r hset hset:update name bob
        assert_hashtmpl_encoding hset:update
        assert_equal [r hget hset:update name] bob
    }

    test {HSET multiple fields on template-based hash} {
        make_hashtmpl hset:multi a 1
        r hset hset:multi b 2 c 3 d 4
        assert_hashtmpl_encoding hset:multi
        assert_equal [r hlen hset:multi] 4
    }

    # ============================================================
    # HSCAN on template-based hash
    # ============================================================

    test {HSCAN on template-based hash returns all fields} {
        make_hashtmpl hscan:test a 1 b 2 c 3
        set result [r hscan hscan:test 0]
        set cursor [lindex $result 0]
        set elements [lindex $result 1]
        assert_equal $cursor 0
        assert_equal [llength $elements] 6
    }

    test {HSCAN with MATCH pattern on template-based hash} {
        make_hashtmpl hscan:match field1 val1 field2 val2 other val3
        set result [r hscan hscan:match 0 MATCH field*]
        set elements [lindex $result 1]
        # Should match field1 and field2
        assert {[llength $elements] == 4}
    }

    test {HSCAN with NOVALUES on template-based hash} {
        make_hashtmpl hscan:noval a 1 b 2 c 3
        set result [r hscan hscan:noval 0 NOVALUES]
        set elements [lindex $result 1]
        assert_equal [llength $elements] 3
        assert_equal [lsort $elements] {a b c}
    }

    # ============================================================
    # HRANDFIELD on template-based hash
    # ============================================================

    test {HRANDFIELD on template-based hash} {
        make_hashtmpl hrand:test a 1 b 2 c 3
        set field [r hrandfield hrand:test]
        assert {$field eq "a" || $field eq "b" || $field eq "c"}
    }

    test {HRANDFIELD with count on template-based hash} {
        make_hashtmpl hrand:count a 1 b 2 c 3 d 4
        set fields [r hrandfield hrand:count 2]
        assert_equal [llength $fields] 2
    }

    test {HRANDFIELD with WITHVALUES on template-based hash} {
        make_hashtmpl hrand:withval a 1 b 2
        set result [r hrandfield hrand:withval 2 WITHVALUES]
        assert_equal [llength $result] 4
    }

    test {HRANDFIELD negative count on template-based hash} {
        make_hashtmpl hrand:neg a 1 b 2
        set result [r hrandfield hrand:neg -5]
        assert_equal [llength $result] 5
    }

    # ============================================================
    # Hash Field Expiration - converts to listpackex
    # ============================================================

    # HFE deconversion target depends on hash-max-listpack-entries; under the
    # TMPL_AR iter (entries=0) the target is "hashtable" instead of
    # "listpackex". Run only in the LP iter to keep the assertion strict.
    if {$encoding eq "template-listpack"} {
    test {HPEXPIRE on template-based hash converts to listpackex} {
        make_hashtmpl hfe:test name alice email alice@example.com
        assert_hashtmpl_encoding hfe:test
        r hpexpire hfe:test 100000 FIELDS 1 name
        assert_match "*encoding:listpackex*" [r debug object hfe:test]
        assert_equal [r hget hfe:test name] alice
        assert_equal [r hget hfe:test email] alice@example.com
    }

    test {HEXPIRE on template-based hash converts to listpackex} {
        make_hashtmpl hfe:expire name bob age 30
        assert_hashtmpl_encoding hfe:expire
        r hexpire hfe:expire 100 FIELDS 1 age
        assert_match "*encoding:listpackex*" [r debug object hfe:expire]
    }
    } ;# end if template-listpack

    test {HSETEX without expiration keeps template encoding} {
        make_hashtmpl hfe:noexp name alice
        # HSET without expiration should keep template-based encoding
        r hset hfe:noexp email alice@example.com
        assert_hashtmpl_encoding hfe:noexp
    }

    # ============================================================
    # DUMP/RESTORE on template-based hash
    # ============================================================

    test {DUMP/RESTORE preserves template-based hash} {
        make_hashtmpl dump:test name alice email alice@example.com age 25
        assert_hashtmpl_encoding dump:test
        set dump [r dump dump:test]
        r del dump:test
        r restore dump:test 0 $dump
        assert_hashtmpl_encoding dump:test
        # Length-first sort: age (3) < name (4) < email (5)
        assert_equal [r hgetall dump:test] {age 25 name alice email alice@example.com}
    }

    test {DUMP/RESTORE to different key} {
        make_hashtmpl dump:src a 1 b 2 c 3
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

    test {INFO stats shows hash template stats} {
        r flushall
        # Key-refs from a flushall are released on a BIO lazyfree thread, so the
        # live key count is eventually consistent; poll for the clean baseline
        # left by prior tests.
        wait_hashtmpl_keys 0

        # Create 3 keys with same template
        make_hashtmpl info:k1 a 1 b 2
        make_hashtmpl info:k2 a 3 b 4
        make_hashtmpl info:k3 a 5 b 6
        lassign [get_template_stats] tpl1 keys1
        assert_equal $keys1 3

        # Create 1 key with different template
        make_hashtmpl info:k4 x 1 y 2 z 3
        lassign [get_template_stats] tpl2 keys2
        assert_equal $keys2 4

        # Delete one key
        r del info:k1
        lassign [get_template_stats] tpl3 keys3
        assert_equal $keys3 3

        # Flushall resets to 0 (key-refs released on a BIO lazyfree thread)
        r flushall
        wait_hashtmpl_keys 0
    }

    # ============================================================
    # registry shrink
    # ============================================================

    test {registry frees by_id when the last template is removed} {
        # Drain to an empty registry: drop all keys (key-refs) and every prepared
        # template (hold-refs) accumulated by earlier tests on this connection.
        # Emptying the registry is the path that frees the by_id array.
        r flushall
        r himport discardall
        wait_hashtmpl_templates 0

        make_hashtmpl shrink:k a 1 b 2 c 3
        assert {[lindex [get_template_stats] 0] >= 1}

        # Drop both refs so the registry empties again.
        r del shrink:k
        r himport discardall
        wait_hashtmpl_templates 0

        # by_id was freed (capacity reset to 0); a fresh template must still be
        # creatable, exercising the grow-from-NULL path in allocateTemplateId.
        make_hashtmpl shrink:k2 a 1 b 2 c 3
        assert_hashtmpl_encoding shrink:k2
        assert_equal [r hget shrink:k2 a] 1

        r del shrink:k2
        r himport discardall
    }

    # ============================================================
    # FLUSHALL / FLUSHDB - async free safety
    # ============================================================

    test {FLUSHALL with template-based hashes does not crash} {
        make_hashtmpl flush:test1 a 1 b 2 c 3
        make_hashtmpl flush:test2 a 4 b 5 c 6
        make_hashtmpl flush:test3 x 1 y 2
        assert_hashtmpl_encoding flush:test1
        r flushall
        assert_equal [r dbsize] 0
        # Create new hashes after flush
        make_hashtmpl flush:new a 1 b 2
        assert_hashtmpl_encoding flush:new
        assert_equal [r hget flush:new a] 1
    }

    test {FLUSHDB with template-based hashes does not crash} {
        r select 1
        make_hashtmpl flushdb:test a 1 b 2
        assert_hashtmpl_encoding flushdb:test
        r flushdb
        assert_equal [r dbsize] 0
        r select 0
    }

    test {Multiple FLUSHALL in succession does not crash} {
        make_hashtmpl multi:flush a 1 b 2
        r flushall
        r flushall
        r flushall
        r ping
    } {PONG}

    # ============================================================
    # DEL - template refcount management
    # ============================================================

    test {DEL on template-based hash releases template ref} {
        make_hashtmpl del:test name alice email bob
        assert_hashtmpl_encoding del:test
        r del del:test
        assert_equal [r exists del:test] 0
        # Should be able to create new hash with same template
        make_hashtmpl del:new name charlie email dave
        assert_hashtmpl_encoding del:new
    }

    # ============================================================
    # HSETNX on template-based hash
    # ============================================================

    test {HSETNX on existing field in template-based hash} {
        make_hashtmpl hsetnx:test name alice
        assert_equal [r hsetnx hsetnx:test name bob] 0
        assert_equal [r hget hsetnx:test name] alice
    }

    test {HSETNX on new field in template-based hash} {
        make_hashtmpl hsetnx:new name alice
        assert_equal [r hsetnx hsetnx:new email alice@example.com] 1
        assert_hashtmpl_encoding hsetnx:new
        assert_equal [r hget hsetnx:new email] alice@example.com
    }

    # ============================================================
    # Key-level expiration on template-based hash
    # ============================================================

    test {EXPIRE on template-based hash works} {
        make_hashtmpl expire:test name alice
        r expire expire:test 100
        set ttl [r ttl expire:test]
        assert {$ttl > 0 && $ttl <= 100}
        assert_hashtmpl_encoding expire:test
    }

    # ============================================================
    # COPY command on template-based hash
    # ============================================================

    test {COPY template-based hash to new key} {
        make_hashtmpl copy:src name alice email bob
        assert_hashtmpl_encoding copy:src
        r copy copy:src copy:dst
        assert_hashtmpl_encoding copy:dst
        assert_equal [r hgetall copy:dst] [r hgetall copy:src]
    }

    test {COPY template-based hash with REPLACE} {
        make_hashtmpl copy:replace:src a 1 b 2
        r set copy:replace:dst "string"
        r copy copy:replace:src copy:replace:dst REPLACE
        assert_equal [r type copy:replace:dst] hash
        assert_hashtmpl_encoding copy:replace:dst
    }

}

# ============================================================
# RDB SAVE/LOAD tests (require server restart)
# ============================================================

start_server {tags {"hash" "hinted-hash-templates" "rdb" "needs:debug"}
              overrides {hash-min-template-entries 0}} {

    test {RDB save and load preserves template-based hash} {
        make_hashtmpl rdb:test name alice email alice@example.com age 25
        assert_hashtmpl_encoding rdb:test

        r debug reload

        assert_hashtmpl_encoding rdb:test
        # Length-first sort: age (3) < name (4) < email (5)
        assert_equal [r hgetall rdb:test] {age 25 name alice email alice@example.com}
    }

    test {RDB save and load multiple template-based hashes with shared template} {
        r flushall
        make_hashtmpl rdb:multi1 a 1 b 2 c 3
        make_hashtmpl rdb:multi2 a 4 b 5 c 6
        make_hashtmpl rdb:multi3 x 1 y 2

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

start_server {tags {"hash" "hinted-hash-templates" "repl" "needs:repl" "needs:debug" "cluster:skip" "external:skip"}
              overrides {hash-min-template-entries 0}} {
    start_server {overrides {hash-min-template-entries 0}} {
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
            set master_enc [$master object encoding repl:test]
            set replica_enc [$replica object encoding repl:test]

            assert {$master_enc eq "template-listpack" ||
                    $master_enc eq "template-array"}
            assert {$replica_enc eq "template-listpack" ||
                    $replica_enc eq "template-array"}
            # Length-first sort: name (4) < email (5)
            assert_equal [$replica hgetall repl:test] {name alice email alice@example.com}
        }
    }
}

# A diskless replica reaches rdbLoadRioWithLoadingCtx() directly, bypassing
# rdbLoadRio(). The load-time template registry must be released after each load
# so that a second full resync does not hit a stale "Duplicate hash template
# ID". Force two full resyncs and assert the replica stays intact.
start_server {tags {"hash" "hinted-hash-templates" "repl" "needs:repl" "needs:debug" "cluster:skip" "external:skip"}
              overrides {hash-min-template-entries 0 repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    start_server {overrides {hash-min-template-entries 0 repl-diskless-load swapdb}} {
        test "Diskless replica survives repeated full resyncs with templates ($encoding)" {
            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set replica [srv 0 client]

            if {$encoding eq "template-array"} {
                $master config set hash-max-listpack-entries 0
            }
            $master flushall
            $master himport prepare u a b c
            $master himport set dr:k1 u 1 2 3
            $master himport set dr:k2 u 4 5 6

            # Full resync #1 (diskless load into an empty db).
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            assert_equal [$replica hget dr:k1 a] 1
            set tmpls [s 0 hash_templates]
            set keys [s 0 hash_template_keys]

            # Force full resync #2 (changing the master replid defeats partial
            # resync) so the template registry is loaded a second time.
            $replica replicaof no one
            $master debug change-repl-id
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            assert_equal PONG [$replica ping]
            assert_equal [$replica hget dr:k1 a] 1
            assert_equal [$replica hget dr:k2 c] 6
            assert_equal $tmpls [s 0 hash_templates]
            # The old dataset's key-refs are released by a BIO lazyfree thread,
            # so the count settles back asynchronously after the resync.
            wait_hashtmpl_keys $keys 0
            assert {[s -1 sync_full] >= 2}
        }
    }
}

# ============================================================
# AOF rewrite tests
# ============================================================

start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip" "external:skip"}
              overrides {save {} appendonly yes auto-aof-rewrite-percentage 0
                         hash-min-template-entries 0}} {

    foreach rdbpre {yes no} {
        test "AOF rewrite preserves template encoding (rdb-preamble=$rdbpre)" {
            r config set aof-use-rdb-preamble $rdbpre
            r flushall
            # Drain deferred key-ref releases left by the previous test so the
            # baseline below counts only the keys created here.
            wait_hashtmpl_keys 0
            waitForBgrewriteaof r

            make_hashtmpl aof:k1 a 1 b 2 c 3
            make_hashtmpl aof:k2 a 4 b 5 c 6
            make_hashtmpl aof:k3 x 7 y 8

            set enc1 [r object encoding aof:k1]
            assert {$enc1 eq "template-listpack" || $enc1 eq "template-array"}
            set tmpls_before [s hash_templates]
            set keys_before [s hash_template_keys]

            r bgrewriteaof
            waitForBgrewriteaof r

            r debug loadaof

            set enc1r [r object encoding aof:k1]
            set enc2r [r object encoding aof:k2]
            set enc3r [r object encoding aof:k3]
            assert {$enc1r eq "template-listpack" || $enc1r eq "template-array"}
            assert {$enc2r eq "template-listpack" || $enc2r eq "template-array"}
            assert {$enc3r eq "template-listpack" || $enc3r eq "template-array"}

            assert_equal $tmpls_before [s hash_templates]
            wait_hashtmpl_keys $keys_before

            assert_equal [r hget aof:k1 a] 1
            assert_equal [r hget aof:k2 b] 5
            assert_equal [r hget aof:k3 y] 8
        }
    }

    test {AOF rewrite mixes template and plain hashes correctly} {
        r config set aof-use-rdb-preamble no
        r flushall
        waitForBgrewriteaof r

        make_hashtmpl mix:tmpl1 a 1 b 2 c 3
        make_hashtmpl mix:tmpl2 a 4 b 5 c 6
        r hset mix:plain1 x 100 y 200
        r hset mix:plain2 m 1 n 2 o 3

        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof

        set enc_t1 [r object encoding mix:tmpl1]
        set enc_p1 [r object encoding mix:plain1]
        assert {$enc_t1 eq "template-listpack" || $enc_t1 eq "template-array"}
        assert_equal $enc_p1 "listpack"
        assert_equal [r hget mix:tmpl1 b] 2
        assert_equal [r hget mix:plain1 x] 100
    }

    # The load-time template registry (rdb_tmpls in rdb.c) must be released
    # after every RDB load so that loading the same dataset more than once does
    # not see a stale "Duplicate hash template ID". The AOF-preamble path loads
    # via rdbLoadRio(); exercise it by reloading several times in a row.
    test {Repeated AOF rdb-preamble loads keep templates stable} {
        r config set aof-use-rdb-preamble yes
        if {$encoding eq "template-array"} { r config set hash-max-listpack-entries 0 }
        r flushall
        # Drain deferred key-ref releases from the previous test first.
        wait_hashtmpl_keys 0
        waitForBgrewriteaof r

        make_hashtmpl aofreload:k1 a 1 b 2 c 3
        make_hashtmpl aofreload:k2 a 4 b 5 c 6
        set tmpls_before [s hash_templates]
        set keys_before [s hash_template_keys]

        r bgrewriteaof
        waitForBgrewriteaof r

        for {set i 0} {$i < 3} {incr i} {
            r debug loadaof
            assert_equal PONG [r ping]
            assert_equal $tmpls_before [s hash_templates]
            wait_hashtmpl_keys $keys_before
        }
        assert_equal [r hget aofreload:k1 a] 1
        assert_equal [r hget aofreload:k2 c] 6
    }
}
} ;# end foreach encoding

# Disk-based replica (repl-diskless-load disabled): the received RDB is written
# to disk and loaded via rdbLoad(). A second full resync must not leak the
# load-time template registry (mirrors the diskless test for the disk path).
start_server {tags {"hash" "hinted-hash-templates" "repl" "needs:repl" "needs:debug" "cluster:skip" "external:skip"}
              overrides {hash-min-template-entries 0 repl-diskless-sync no}} {
    start_server {overrides {hash-min-template-entries 0 repl-diskless-load disabled}} {
        test {Disk-based replica survives repeated full resyncs with templates} {
            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set replica [srv 0 client]

            $master flushall
            $master himport prepare u a b c
            $master himport set dr:k1 u 1 2 3
            $master himport set dr:k2 u 4 5 6

            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            assert_equal [$replica hget dr:k1 a] 1
            set tmpls [s 0 hash_templates]
            set keys [s 0 hash_template_keys]

            # Force full resync #2 (changing the master replid defeats partial
            # resync) so the template registry is loaded a second time.
            $replica replicaof no one
            $master debug change-repl-id
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            assert_equal PONG [$replica ping]
            assert_equal [$replica hget dr:k1 a] 1
            assert_equal [$replica hget dr:k2 c] 6
            assert_equal $tmpls [s 0 hash_templates]
            # Old dataset key-refs are released asynchronously by the replica's
            # BIO lazyfree thread after the resync.
            wait_hashtmpl_keys $keys 0
            assert {[s -1 sync_full] >= 2}
        }
    }
}

# Chained replication A->B->C: HSETC (the propagated form of HIMPORT SET) must
# flow down the chain and reconstruct the template hash on every node.
start_server {tags {"hash" "hinted-hash-templates" "repl" "needs:repl" "cluster:skip" "external:skip"}
              overrides {hash-min-template-entries 0}} {
    start_server {overrides {hash-min-template-entries 0}} {
        start_server {overrides {hash-min-template-entries 0}} {
            test {Chained replication propagates template hashes A->B->C} {
                set a [srv -2 client]
                set a_host [srv -2 host]
                set a_port [srv -2 port]
                set b [srv -1 client]
                set b_host [srv -1 host]
                set b_port [srv -1 port]
                set c [srv 0 client]

                $b replicaof $a_host $a_port
                $c replicaof $b_host $b_port
                wait_for_sync $b
                wait_for_sync $c

                $a himport prepare u name email
                $a himport set chain:1 u alice alice@x.com

                wait_for_condition 50 100 { [$c exists chain:1] == 1 } else {
                    fail "template hash not propagated to C"
                }
                assert_equal [$c hgetall chain:1] {name alice email alice@x.com}
                assert_equal [$c object encoding chain:1] template-listpack
                assert_equal [$b hget chain:1 email] alice@x.com
            }
        }
    }
}

# hashTypeTryConvertToTemplate() must honor the disabled (0) sentinel: a plain
# hash loaded from RDB converts to a template encoding only when the feature is
# enabled (> 0), never at the default 0.
start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip" "external:skip"}
              overrides {hash-min-template-entries 0 hash-max-listpack-entries 64 appendonly no}} {
    # "small" stays listpack (<= hash-max-listpack-entries), "big" is hashtable.
    test {hash-min-template-entries=0: restart keeps plain hashes unconverted} {
        r flushall
        for {set i 0} {$i < 5}   {incr i} { r hset small f$i v$i }
        for {set i 0} {$i < 200} {incr i} { r hset big f$i v$i }
        assert_equal listpack  [r object encoding small]
        assert_equal hashtable [r object encoding big]
        r save
        restart_server 0 true false
        assert_equal listpack  [r object encoding small]
        assert_equal hashtable [r object encoding big]
        assert_equal 0 [s hash_templates]
        assert_equal v3   [r hget small f3]
        assert_equal v100 [r hget big f100]
    }

    test {hash-min-template-entries>0: restart converts listpack to TMPL_LP and hashtable to TMPL_ARRAY} {
        r flushall
        for {set i 0} {$i < 5}   {incr i} { r hset small f$i v$i }
        for {set i 0} {$i < 200} {incr i} { r hset big f$i v$i }
        assert_equal 0 [s hash_templates]
        # Persist config so the restarted server loads with the feature enabled.
        r config set hash-min-template-entries 4
        r config rewrite
        r save
        restart_server 0 true false
        # Both plain hashes were converted at load time: small via the listpack
        # path, big via the hashtable path.
        assert_equal template-listpack [r object encoding small]
        assert_equal template-array    [r object encoding big]
        assert_equal 2 [s hash_templates]
        assert_equal v3   [r hget small f3]
        assert_equal v100 [r hget big f100]
    }

    test {hash-min-template-entries>0: RESTORE converts plain listpack and hashtable hashes} {
        r config set hash-min-template-entries 0
        r flushall
        wait_for_condition 50 20 { [s hash_templates] == 0 } else {
            fail "templates not drained after flushall"
        }
        for {set i 0} {$i < 5}   {incr i} { r hset src_lp f$i v$i }
        for {set i 0} {$i < 200} {incr i} { r hset src_ht f$i v$i }
        assert_equal listpack  [r object encoding src_lp]
        assert_equal hashtable [r object encoding src_ht]
        set lp_payload [r dump src_lp]
        set ht_payload [r dump src_ht]
        r config set hash-min-template-entries 4
        r restore dst_lp 0 $lp_payload
        r restore dst_ht 0 $ht_payload
        assert_equal template-listpack [r object encoding dst_lp]
        assert_equal template-array    [r object encoding dst_ht]
        assert_equal 2 [s hash_templates]
        assert_equal v3   [r hget dst_lp f3]
        assert_equal v100 [r hget dst_ht f100]
    }
}

# ============================================================
# Tests under hash-min-template-entries=1 (production default).
# In this mode plain HSET auto-converts to a template encoding, which
# OBJECT ENCODING / DEBUG OBJECT report as template-listpack / template-array
# (the legacy listpack/hashtable name is only reported when the
# hash-template-mask-encoding compat config is set).
# ============================================================
start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip"}
              overrides {hash-min-template-entries 1}} {

    # Template free is deferred to serverCron (every ~1s). After flushall,
    # poll until the registry is fully drained so external-server runs see
    # a clean baseline.
    proc wait_tmpl_drain {} {
        wait_for_condition 30 100 {
            [lindex [get_template_stats] 0] == 0
        } else {
            fail "hash template registry did not drain"
        }
    }

    test {threshold=1: HSET auto-converts to template encoding} {
        r flushall
        wait_tmpl_drain
        lassign [get_template_stats] t0 k0
        r hset auto:1 a 1 b 2 c 3
        lassign [get_template_stats] t1 k1
        assert {$t1 > $t0}
        assert_equal [expr {$k1 - $k0}] 1
        assert_equal [r object encoding auto:1] "template-listpack"
        assert_match "*encoding:template-listpack*" [r debug object auto:1]
        assert_equal [r hgetall auto:1] {a 1 b 2 c 3}
    }

    test {threshold=1: same field set shares a single template} {
        r flushall
        wait_tmpl_drain
        r hset shared:1 a 1 b 2 c 3
        lassign [get_template_stats] t1 _
        r hset shared:2 a 9 b 8 c 7
        r hset shared:3 a 0 b 0 c 0
        lassign [get_template_stats] t3 k3
        assert_equal $t1 $t3
        assert_equal $k3 3
        assert_equal [r hget shared:2 b] 8
    }

    test {threshold=1: large hash auto-converts to template-array} {
        r flushall
        wait_tmpl_drain
        # Force a low listpack threshold so the template uses the array variant
        # deterministically regardless of test defaults.
        set saved [lindex [r config get hash-max-listpack-entries] 1]
        r config set hash-max-listpack-entries 16
        set cmd [list r hset big:1]
        for {set i 0} {$i < 32} {incr i} { lappend cmd "f$i" "v$i" }
        {*}$cmd
        assert_equal [r object encoding big:1] "template-array"
        assert_equal [r hget big:1 f10] "v10"
        assert_equal [r hlen big:1] 32
        r config set hash-max-listpack-entries $saved
    }

    test {threshold=1: HFE prevents template conversion} {
        r flushall
        wait_tmpl_drain
        r hset hfe:1 a 1 b 2 c 3
        # Auto-converted to template; HEXPIRE forces back to listpackex.
        r hexpire hfe:1 100 FIELDS 1 a
        assert_equal [r object encoding hfe:1] "listpackex"
        # New hash created with HFE from start: never templates.
        r hsetex hfe:2 EX 100 FIELDS 1 a 1
        assert_equal [r object encoding hfe:2] "listpackex"
    }

    test {threshold=1: RDB save/load preserves template-converted hashes} {
        r flushall
        wait_tmpl_drain
        r hset rdb:k1 a 1 b 2 c 3
        r hset rdb:k2 a 9 b 8 c 7
        lassign [get_template_stats] t_before k_before
        r debug reload
        lassign [get_template_stats] t_after k_after
        assert_equal $t_before $t_after
        assert_equal $k_before $k_after
        assert_equal [r hgetall rdb:k1] {a 1 b 2 c 3}
        assert_equal [r hget rdb:k2 b] 8
        assert_equal [r object encoding rdb:k1] "template-listpack"
    }

    test {threshold=1: HDEL releases template ref when key is dropped} {
        r flushall
        wait_tmpl_drain
        r hset del:1 a 1 b 2 c 3
        lassign [get_template_stats] _ k1
        r del del:1
        lassign [get_template_stats] _ k2
        assert_equal [expr {$k1 - $k2}] 1
    }

    test {threshold=1: AOF rewrite preserves auto-converted hashes} {
        r config set aof-use-rdb-preamble no
        r flushall
        wait_tmpl_drain
        waitForBgrewriteaof r
        r hset aof:1 a 1 b 2 c 3
        r hset aof:2 a 9 b 8 c 7
        lassign [get_template_stats] t_before k_before
        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof
        lassign [get_template_stats] t_after k_after
        assert_equal $t_before $t_after
        assert_equal $k_before $k_after
        assert_equal [r hgetall aof:1] {a 1 b 2 c 3}
        assert_equal [r object encoding aof:1] "template-listpack"
    }
}


# ============================================================
# Encoding conversion path coverage.
# Each test exercises one specific runtime conversion path
# between TMPL_LP / TMPL_AR / LISTPACK / LISTPACK_EX / HT.
# ============================================================
start_server {tags {"hash" "hinted-hash-templates" "convert" "needs:debug" "cluster:skip"}
              overrides {hash-min-template-entries 0
                         hash-max-listpack-entries 8
                         hash-max-listpack-value 64}} {

    test {convert: TMPL_LP -> TMPL_AR via HSET large value} {
        # In-place update of an existing field with a value over
        # hash-max-listpack-value (64) -> must escalate to TMPL_ARRAY.
        r himport prepare t5_tpl a b c d
        r himport set t5 t5_tpl 1 2 3 4
        assert_equal [r object encoding t5] template-listpack

        r hset t5 a [string repeat x 100]
        assert_equal [r object encoding t5] template-array
        assert_equal [r hget t5 a] [string repeat x 100]

        r del t5
        r himport discard t5_tpl
    }

    test {convert: TMPL_LP -> TMPL_AR via HSET new fields (count > listpack-entries)} {
        # hash-max-listpack-entries is 8 in this server. Start at the limit.
        set fields {}; set values {}
        for {set i 0} {$i < 8} {incr i} {
            lappend fields "f$i"; lappend values "v$i"
        }
        r himport prepare t5b_tpl {*}$fields
        r himport set t5b t5b_tpl {*}$values
        assert_equal [r object encoding t5b] template-listpack

        # Adding the 9th field via HSET grows a new template and pushes the
        # count past the limit -> must escalate TMPL_LP to TMPL_ARRAY.
        r hset t5b f8 v8
        assert_equal [r object encoding t5b] template-array

        # Data integrity across the conversion.
        assert_equal [r hlen t5b] 9
        for {set i 0} {$i < 9} {incr i} {
            assert_equal [r hget t5b f$i] v$i
        }

        r del t5b
        r himport discard t5b_tpl
    }

    test {convert: TMPL_LP -> TMPL_AR via HSET new field with large value} {
        # Field count stays under hash-max-listpack-entries (8); only the new
        # value crosses hash-max-listpack-value (64) -> must escalate to
        # TMPL_ARRAY, mirroring the value-size rule of a plain listpack hash.
        r himport prepare t5c_tpl a b c d
        r himport set t5c t5c_tpl 1 2 3 4
        assert_equal [r object encoding t5c] template-listpack

        r hset t5c e [string repeat x 100]
        assert_equal [r object encoding t5c] template-array

        # Data integrity across the conversion.
        assert_equal [r hlen t5c] 5
        assert_equal [r hget t5c e] [string repeat x 100]
        assert_equal [r hget t5c a] 1
        assert_equal [r hget t5c d] 4

        r del t5c
        r himport discard t5c_tpl
    }

    test {convert: TMPL_LP -> LISTPACK_EX via HEXPIRE (small)} {
        r himport prepare t6_tpl a b c d
        r himport set t6 t6_tpl 1 2 3 4
        assert_equal [r object encoding t6] template-listpack

        r hexpire t6 100 FIELDS 1 a
        assert_equal [r object encoding t6] listpackex
        assert_equal [r hget t6 b] 2

        r del t6
        r himport discard t6_tpl
    }

    test {convert: TMPL_LP -> HT via HEXPIRE (count > listpack-entries)} {
        set prev_e [lindex [r config get hash-max-listpack-entries] 1]
        r config set hash-max-listpack-entries 32

        set fields {}; set values {}
        for {set i 0} {$i < 16} {incr i} {
            lappend fields "f$i"; lappend values "v$i"
        }
        r himport prepare t7_tpl {*}$fields
        r himport set t7 t7_tpl {*}$values
        assert_equal [r object encoding t7] template-listpack

        # Tighten limit so HFE-trigger escalates LP_EX -> HT.
        r config set hash-max-listpack-entries 4
        r hexpire t7 100 FIELDS 1 f0
        assert_equal [r object encoding t7] hashtable
        assert_equal [r hget t7 f5] v5

        r del t7
        r himport discard t7_tpl
        r config set hash-max-listpack-entries $prev_e
    }

    test {convert: TMPL_AR -> LISTPACK_EX via HEXPIRE (small)} {
        set prev_e [lindex [r config get hash-max-listpack-entries] 1]
        # Force TMPL_AR at create time.
        r config set hash-max-listpack-entries 0
        r himport prepare t8_tpl a b c d
        r himport set t8 t8_tpl 1 2 3 4
        r config set hash-max-listpack-entries $prev_e
        assert_equal [r object encoding t8] template-array

        r hexpire t8 100 FIELDS 1 a
        assert_equal [r object encoding t8] listpackex
        assert_equal [r hget t8 b] 2

        r del t8
        r himport discard t8_tpl
    }

    test {convert: TMPL_AR -> HT via HEXPIRE (count > listpack-entries)} {
        set prev_e [lindex [r config get hash-max-listpack-entries] 1]
        # Force TMPL_AR at create time (entries=0 fails fits-listpack check).
        r config set hash-max-listpack-entries 0
        set fields {}; set values {}
        for {set i 0} {$i < 16} {incr i} {
            lappend fields "f$i"; lappend values "v$i"
        }
        r himport prepare t9_tpl {*}$fields
        r himport set t9 t9_tpl {*}$values
        assert_equal [r object encoding t9] template-array

        # Raise limit just enough that count(16) still > limit -> escalates to HT.
        r config set hash-max-listpack-entries 4
        r hexpire t9 100 FIELDS 1 f0
        assert_equal [r object encoding t9] hashtable
        assert_equal [r hget t9 f7] v7

        r del t9
        r himport discard t9_tpl
        r config set hash-max-listpack-entries $prev_e
    }

    test {convert: TMPL_LP -> LISTPACK via auto-trim (fits)} {
        set prev_m [lindex [r config get hash-min-template-entries] 1]
        r himport prepare t10_tpl a b c d
        r himport set t10 t10_tpl 1 2 3 4
        assert_equal [r object encoding t10] template-listpack

        # Raise threshold so HDEL drops count below min and triggers trim.
        r config set hash-min-template-entries 4
        r hdel t10 a
        assert_equal [r object encoding t10] listpack
        assert_equal [r hget t10 b] 2

        r del t10
        r himport discard t10_tpl
        r config set hash-min-template-entries $prev_m
    }

    test {convert: TMPL_LP -> HT via auto-trim (count > listpack-entries)} {
        set prev_e [lindex [r config get hash-max-listpack-entries] 1]
        set prev_m [lindex [r config get hash-min-template-entries] 1]
        r config set hash-max-listpack-entries 32

        set fields {}; set values {}
        for {set i 0} {$i < 8} {incr i} {
            lappend fields "f$i"; lappend values "v$i"
        }
        r himport prepare t11_tpl {*}$fields
        r himport set t11 t11_tpl {*}$values
        assert_equal [r object encoding t11] template-listpack

        # Tighten limit and raise trim threshold; HDEL fails CanConvert -> HT.
        r config set hash-max-listpack-entries 4
        r config set hash-min-template-entries 10
        r hdel t11 f0
        assert_equal [r object encoding t11] hashtable
        assert_equal [r hget t11 f3] v3

        r del t11
        r himport discard t11_tpl
        r config set hash-max-listpack-entries $prev_e
        r config set hash-min-template-entries $prev_m
    }

    test {convert: TMPL_AR -> LISTPACK via auto-trim (fits)} {
        set prev_e [lindex [r config get hash-max-listpack-entries] 1]
        set prev_m [lindex [r config get hash-min-template-entries] 1]
        r config set hash-max-listpack-entries 0
        r himport prepare t12_tpl a b c d
        r himport set t12 t12_tpl 1 2 3 4
        r config set hash-max-listpack-entries $prev_e
        assert_equal [r object encoding t12] template-array

        # HDEL one field -> count(3) < min(4) -> trim; values fit -> LP.
        r config set hash-min-template-entries 4
        r hdel t12 a
        assert_equal [r object encoding t12] listpack
        assert_equal [r hget t12 c] 3

        r del t12
        r himport discard t12_tpl
        r config set hash-min-template-entries $prev_m
    }

    test {convert: TMPL_AR -> HT via auto-trim (value > listpack-value)} {
        set prev_v [lindex [r config get hash-max-listpack-value] 1]
        set prev_m [lindex [r config get hash-min-template-entries] 1]
        r config set hash-max-listpack-value 10
        # 50-char value forces TMPL_AR at create and fails the trim CanConvert.
        r himport prepare t13_tpl a b c d
        r himport set t13 t13_tpl 1 2 3 [string repeat y 50]
        assert_equal [r object encoding t13] template-array

        r config set hash-min-template-entries 4
        r hdel t13 a
        assert_equal [r object encoding t13] hashtable
        assert_equal [r hget t13 d] [string repeat y 50]

        r del t13
        r himport discard t13_tpl
        r config set hash-max-listpack-value $prev_v
        r config set hash-min-template-entries $prev_m
    }
}

# ============================================================
# Memory savings. Many identical hashes that share one schema must be far
# cheaper as templates, because the field names are stored once in the shared
# template instead of once per key. Each test creates 20000 identical hashes in
# both forms and requires the template form to be at least 40% smaller, both
# for a single key (MEMORY USAGE) and across all keys (used_memory).
# ============================================================
start_server {tags {"hash" "hinted-hash-templates" "memory" "needs:debug" "cluster:skip"}
              overrides {hash-min-template-entries 0}} {
    # Every hash holds the same 32 fields. Field names and values are the same
    # length, so in a plain hash exactly half the payload is field-name bytes,
    # which is the part a template stores once instead of once per key.
    set field_names {}
    set field_values {}
    set hset_pairs {}
    for {set i 0} {$i < 32} {incr i} {
        set name  [format "shared_field_name_%02d" $i]
        set value [format "shared_value_num_%03d" $i]
        lappend field_names  $name
        lappend field_values $value
        lappend hset_pairs   $name $value
    }

    test {TMPL_LP is 40%+ smaller than listpack (single key and 20000 total)} {
        r flushall
        r config set hash-max-listpack-entries 128   ;# 32 fields stay listpack

        # 20000 plain listpack hashes; record total growth and one key's size.
        set mem_before [s used_memory]
        set rd [redis_deferring_client]
        for {set i 0} {$i < 20000} {incr i} { $rd hset plain:$i {*}$hset_pairs }
        for {set i 0} {$i < 20000} {incr i} { $rd read }
        $rd close
        assert_equal [r object encoding plain:0] listpack
        set plain_total  [expr {[s used_memory] - $mem_before}]
        set plain_single [r memory usage plain:0]

        r flushall

        # 20000 template hashes sharing one schema (PREPARE once, same connection).
        set mem_before [s used_memory]
        set rd [redis_deferring_client]
        $rd himport prepare schema {*}$field_names; $rd read
        for {set i 0} {$i < 20000} {incr i} { $rd himport set tmpl:$i schema {*}$field_values }
        for {set i 0} {$i < 20000} {incr i} { $rd read }
        $rd close
        assert_equal [r object encoding tmpl:0] template-listpack
        assert_equal [r hget tmpl:0 shared_field_name_00] shared_value_num_000
        set tmpl_total  [expr {[s used_memory] - $mem_before}]
        set tmpl_single [r memory usage tmpl:0]

        assert {$tmpl_single <= $plain_single * 0.6}
        assert {$tmpl_total  <= $plain_total  * 0.6}
    }

    test {TMPL_ARRAY is 40%+ smaller than hashtable (single key and 20000 total)} {
        r flushall
        r config set hash-max-listpack-entries 0     ;# force hashtable / array

        set mem_before [s used_memory]
        set rd [redis_deferring_client]
        for {set i 0} {$i < 20000} {incr i} { $rd hset plain:$i {*}$hset_pairs }
        for {set i 0} {$i < 20000} {incr i} { $rd read }
        $rd close
        assert_equal [r object encoding plain:0] hashtable
        set plain_total  [expr {[s used_memory] - $mem_before}]
        set plain_single [r memory usage plain:0]

        r flushall

        set mem_before [s used_memory]
        set rd [redis_deferring_client]
        $rd himport prepare schema {*}$field_names; $rd read
        for {set i 0} {$i < 20000} {incr i} { $rd himport set tmpl:$i schema {*}$field_values }
        for {set i 0} {$i < 20000} {incr i} { $rd read }
        $rd close
        assert_equal [r object encoding tmpl:0] template-array
        assert_equal [r hget tmpl:0 shared_field_name_00] shared_value_num_000
        set tmpl_total  [expr {[s used_memory] - $mem_before}]
        set tmpl_single [r memory usage tmpl:0]

        assert {$tmpl_single <= $plain_single * 0.6}
        assert {$tmpl_total  <= $plain_total  * 0.6}
    }
}

# ============================================================
# RESTORE deep validation (sanitize-dump-payload yes).
# A template payload must carry strictly sorted field names; the
# field lookup binary search depends on it and duplicate fields are
# illegal, mirroring the duplicate-field checks done for regular
# hashes. Out-of-order or duplicate fields must be rejected.
# ============================================================
start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip"}
              overrides {hash-min-template-entries 0
                         sanitize-dump-payload yes
                         loglevel debug}} {

    # Patching field-name bytes invalidates the CRC footer; skip the
    # checksum so the deep field validation is what rejects the payload.
    r debug set-skip-checksum-validation 1

    # Build a fresh TMPL_LP / TMPL_ARRAY dump for fields field1,field2 -> 1,2.
    # The field names are long and distinctive so the byte substitutions below
    # only ever hit the field-name bytes, never the value or footer bytes.
    proc tmpl_dump {enc} {
        r del rk
        catch {r himport discard rtpl}
        r himport prepare rtpl field1 field2
        r himport set rk rtpl 1 2
        assert_equal [r object encoding rk] $enc
        set dump [r dump rk]
        r del rk
        r himport discard rtpl
        return $dump
    }

    foreach {enc maxlp} {template-listpack 128 template-array 0} {
        r config set hash-max-listpack-entries $maxlp

        # The RESTORE reply is always the generic "Bad data format", so we
        # assert the specific reason via the rdbReportCorruptRDB log message.
        test "RESTORE deep validation: $enc accepts sorted fields" {
            set dump [tmpl_dump $enc]
            r restore rk 0 $dump
            assert_equal [r object encoding rk] $enc
            assert_equal [r hgetall rk] {field1 1 field2 2}
            r del rk
        }

        test "RESTORE deep validation: $enc rejects out-of-order fields" {
            set dump [tmpl_dump $enc]
            # Swap the names so the stored fields become descending (field2, field1).
            set bad [string map {field1 field2 field2 field1} $dump]
            set loglines [count_log_lines 0]
            assert_error "*Bad data format*" {r restore rk 0 $bad}
            wait_for_log_messages 0 {"*fields not strictly sorted*"} $loglines 50 100
        }

        test "RESTORE deep validation: $enc rejects duplicate fields" {
            set dump [tmpl_dump $enc]
            # Collapse field1 -> field2 so both stored fields are identical.
            set bad [string map {field1 field2} $dump]
            set loglines [count_log_lines 0]
            assert_error "*Bad data format*" {r restore rk 0 $bad}
            wait_for_log_messages 0 {"*fields not strictly sorted*"} $loglines 50 100
        }
    }
}

# Field counts spanning HASH_TMPL_STACK_ENTRIES (128): template code uses a
# stack buffer for <=128 fields and a heap allocation for >128 (himport set,
# createHashObjectFromTemplate, HDEL field-array rebuild). Verify data is
# correct across that boundary, including the binary-search HGET and reload.
start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip"}} {
    foreach fc {127 128 129 256} {
        test "template large field-count correctness: $fc fields" {
            r flushall
            set fields {}
            for {set i 0} {$i < $fc} {incr i} { lappend fields f[format %04d $i] }
            set vals {}
            foreach f $fields { lappend vals val_$f }
            r himport prepare big {*}$fields
            r himport set k big {*}$vals

            assert_match {template-*} [r object encoding k]
            assert_equal $fc [r hlen k]
            # Every field resolves (binary search over the sorted field array,
            # incl. the heap path for fc > 128); a missing field returns nil.
            foreach f $fields { assert_equal val_$f [r hget k $f] }
            assert_equal {} [r hget k nosuchfield]
            assert_equal [expr {$fc * 2}] [llength [r hgetall k]]

            # HDEL one field rebuilds the template (field-array path) and the
            # rest must stay correct.
            assert_equal 1 [r hdel k [lindex $fields 1]]
            assert_equal [expr {$fc - 1}] [r hlen k]
            assert_equal {} [r hget k [lindex $fields 1]]
            assert_equal val_[lindex $fields 0] [r hget k [lindex $fields 0]]
            assert_equal val_[lindex $fields end] [r hget k [lindex $fields end]]

            # Survives RDB round-trip (save/load of a large template).
            r debug reload
            assert_equal [expr {$fc - 1}] [r hlen k]
            assert_equal val_[lindex $fields end] [r hget k [lindex $fields end]]
        }
    }
}

# Race the BIO key-ref drop (FLUSHALL ASYNC) against the main-thread hold-ref
# drop (HIMPORT DISCARDALL) on template free. Probabilistic guard for ASan/TSan.
start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip" "external:skip"}
              overrides {hash-min-template-entries 0
                         lazyfree-lazy-user-flush yes}} {
    test {template lifecycle fuzzer: BIO free races main-thread hold-ref drop} {
        # Unique field names per round so templates fully release on the flush.
        for {set round 0} {$round < 100} {incr round} {
            for {set i 0} {$i < 10} {incr i} {
                if {$i % 3 == 0} {
                    set s {}
                    for {set f 0} {$f < 40} {incr f} { lappend s r${round}_${i}_$f }
                    set vals {}
                    foreach f $s { lappend vals [string repeat v 80] }
                } else {
                    set s [list r${round}_${i}_a r${round}_${i}_b r${round}_${i}_c]
                    set vals {x y z}
                }
                r himport prepare ts$i {*}$s
                r himport set k:$round:$i ts$i {*}$vals
            }
            r flushall async
            for {set p 0} {$p < 5} {incr p} { r ping }
            r himport discardall
        }
        r flushall
        wait_for_condition 200 50 { [s hash_templates] == 0 } else {
            fail "templates did not drain: [s hash_templates]"
        }
        assert_equal PONG [r ping]
    }
}

# Active defrag relocates the per-key allocations of template-encoded hashes
# (the TMPL_LP listpack blob, and the hashTemplateArray struct + its value
# sds's for TMPL_ARRAY). defragKey must update ob->ptr and every values[] entry
# without touching the shared template. Fragment many template hashes, run
# active defrag, and assert the data is byte-identical afterwards (debug digest)
# and the keys still resolve. Requires jemalloc; skipped otherwise.
run_solo {defrag} {
    proc wait_for_template_defrag_stop {maxtries delay} {
        wait_for_condition $maxtries $delay {
            [s active_defrag_running] eq 0
        } else {
            fail "defrag didn't stop (frag [s allocator_frag_ratio])."
        }
    }

    start_server {tags {"defrag" "hinted-hash-templates" "external:skip" "tsan:skip" "needs:debug"}
                  overrides {save "" appendonly no}} {
        if {[string match {*jemalloc*} [s mem_allocator]] && [r debug mallctl arenas.page] <= 8192} {
            test {Active defrag: template-encoded hashes keep data intact} {
                r flushall
                r config set hz 100
                r config set activedefrag no
                r config set active-defrag-threshold-lower 5
                r config set active-defrag-cycle-min 65
                r config set active-defrag-cycle-max 75
                r config set active-defrag-ignore-bytes 100kb
                r config set maxmemory 0
                r config set hash-max-listpack-entries 128

                # Two shared schemas: small -> template-listpack, big -> template-array.
                set big {}
                for {set f 0} {$f < 40} {incr f} { lappend big field_[format %02d $f] }
                set bigval [string repeat x 80]

                set n 20000
                # HIMPORT fieldsets are per-client, so prepare on the same
                # (deferring) connection that issues the HIMPORT SETs.
                set rd [redis_deferring_client]
                $rd himport prepare sm a b c   ; $rd read
                $rd himport prepare bg {*}$big  ; $rd read
                set batch 200
                for {set j 0} {$j < $n} {incr j} {
                    if {$j % 2 == 0} {
                        $rd himport set k:$j sm v${j}a v${j}b v${j}c
                    } else {
                        set vals {}
                        for {set f 0} {$f < 40} {incr f} { lappend vals $bigval }
                        $rd himport set k:$j bg {*}$vals
                    }
                    if {($j + 1) % $batch == 0} {
                        for {set i 0} {$i < $batch} {incr i} { $rd read }
                    }
                }
                for {set j 0} {$j < [expr {$n % $batch}]} {incr j} { $rd read }

                assert_equal template-listpack [r object encoding k:0]
                assert_equal template-array    [r object encoding k:1]

                # Fragment: delete half (j%4<2) so both schemas survive and both
                # are freed, leaving holes in their size classes.
                set deleted 0
                for {set j 0} {$j < $n} {incr j} {
                    if {($j % 4) < 2} { $rd del k:$j; incr deleted }
                }
                for {set j 0} {$j < $deleted} {incr j} { $rd read }
                $rd close

                after 120
                if {$::verbose} { puts "frag before defrag: [s allocator_frag_ratio]" }

                set digest [debug_digest]
                catch {r config set activedefrag yes}
                if {[r config get activedefrag] eq "activedefrag yes"} {
                    wait_for_condition 100 100 {
                        [s total_active_defrag_time] ne 0
                    } else {
                        fail "defrag not started."
                    }
                    wait_for_template_defrag_stop 500 100
                }

                # Data byte-identical after defrag moved the allocations.
                assert_equal $digest [debug_digest]
                # Survivors of both encodings still resolve (k:2 sm, k:3 bg).
                assert_equal template-listpack [r object encoding k:2]
                assert_equal template-array    [r object encoding k:3]
                assert_equal v2a    [r hget k:2 a]
                assert_equal $bigval [r hget k:3 field_00]
                assert_equal $bigval [r hget k:3 field_39]
                r save ;# iterate over all data / pointers
            } {OK}
        }
    }
} ;# run_solo

