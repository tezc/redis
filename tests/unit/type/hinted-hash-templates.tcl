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

    proc assert_listpack_encoding {key} {
        set encoding [r debug object $key]
        assert {[string match "*encoding:listpack*" $encoding]}
    }

    proc assert_listpackex_encoding {key} {
        set encoding [r debug object $key]
        assert {[string match "*encoding:listpackex*" $encoding]}
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
        assert_listpackex_encoding hfe:test
        assert_equal [r hget hfe:test name] alice
        assert_equal [r hget hfe:test email] alice@example.com
    }

    test {HEXPIRE on template-based hash converts to listpackex} {
        make_hashtmpl hfe:expire name bob age 30
        assert_hashtmpl_encoding hfe:expire
        r hexpire hfe:expire 100 FIELDS 1 age
        assert_listpackex_encoding hfe:expire
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
        lassign [get_template_stats] tpl_before keys_before
        assert_equal $keys_before 0

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

        # Flushall resets to 0
        r flushall
        lassign [get_template_stats] tpl_after keys_after
        assert_equal $keys_after 0
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

    proc assert_hashtmpl_encoding {key} {
        set enc [r object encoding $key]
        assert {$enc eq "template-listpack" || $enc eq "template-array"}
    }

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

# ============================================================
# AOF rewrite tests
# ============================================================

start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip" "external:skip"}
              overrides {save {} appendonly yes auto-aof-rewrite-percentage 0
                         hash-min-template-entries 0}} {

    proc make_hashtmpl_aof {key args} {
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

    foreach rdbpre {yes no} {
        test "AOF rewrite preserves template encoding (rdb-preamble=$rdbpre)" {
            r config set aof-use-rdb-preamble $rdbpre
            r flushall
            waitForBgrewriteaof r

            make_hashtmpl_aof aof:k1 a 1 b 2 c 3
            make_hashtmpl_aof aof:k2 a 4 b 5 c 6
            make_hashtmpl_aof aof:k3 x 7 y 8

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
            assert_equal $keys_before [s hash_template_keys]

            assert_equal [r hget aof:k1 a] 1
            assert_equal [r hget aof:k2 b] 5
            assert_equal [r hget aof:k3 y] 8
        }
    }

    test {AOF rewrite mixes template and plain hashes correctly} {
        r config set aof-use-rdb-preamble no
        r flushall
        waitForBgrewriteaof r

        make_hashtmpl_aof mix:tmpl1 a 1 b 2 c 3
        make_hashtmpl_aof mix:tmpl2 a 4 b 5 c 6
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
}
} ;# end foreach encoding

# ============================================================
# Tests under hash-min-template-entries=1 (production default).
# In this mode plain HSET auto-converts to a template encoding and
# OBJECT ENCODING / DEBUG OBJECT report the equivalent non-template
# encoding name ("listpack" / "hashtable") to keep behavior backward
# compatible. The only public signal is INFO stats.
# ============================================================
start_server {tags {"hash" "hinted-hash-templates" "needs:debug" "cluster:skip"}
              overrides {hash-min-template-entries 1}} {

    proc tmpl_stats {} {
        set info [r info stats]
        set t 0; set k 0
        foreach line [split $info "\n"] {
            if {[regexp {^hash_templates:(\d+)} $line -> v]} { set t $v }
            if {[regexp {^hash_template_keys:(\d+)} $line -> v]} { set k $v }
        }
        return [list $t $k]
    }

    # Template free is deferred to serverCron (every ~1s). After flushall,
    # poll until the registry is fully drained so external-server runs see
    # a clean baseline.
    proc wait_tmpl_drain {} {
        wait_for_condition 30 100 {
            [lindex [tmpl_stats] 0] == 0
        } else {
            fail "hash template registry did not drain"
        }
    }

    test {threshold=1: HSET auto-converts to template encoding} {
        r flushall
        wait_tmpl_drain
        lassign [tmpl_stats] t0 k0
        r hset auto:1 a 1 b 2 c 3
        lassign [tmpl_stats] t1 k1
        assert {$t1 > $t0}
        assert_equal [expr {$k1 - $k0}] 1
        # Encoding is masked to "listpack" for backward compat.
        assert_equal [r object encoding auto:1] "listpack"
        assert_match "*encoding:listpack*" [r debug object auto:1]
        assert_equal [r hgetall auto:1] {a 1 b 2 c 3}
    }

    test {threshold=1: same field set shares a single template} {
        r flushall
        wait_tmpl_drain
        r hset shared:1 a 1 b 2 c 3
        lassign [tmpl_stats] t1 _
        r hset shared:2 a 9 b 8 c 7
        r hset shared:3 a 0 b 0 c 0
        lassign [tmpl_stats] t3 k3
        assert_equal $t1 $t3
        assert_equal $k3 3
        assert_equal [r hget shared:2 b] 8
    }

    test {threshold=1: large hash auto-converts and reports as hashtable} {
        r flushall
        wait_tmpl_drain
        # Force a low listpack threshold so equivalent encoding flips to
        # "hashtable" deterministically regardless of test defaults.
        set saved [lindex [r config get hash-max-listpack-entries] 1]
        r config set hash-max-listpack-entries 16
        set cmd [list r hset big:1]
        for {set i 0} {$i < 32} {incr i} { lappend cmd "f$i" "v$i" }
        {*}$cmd
        assert_equal [r object encoding big:1] "hashtable"
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
        lassign [tmpl_stats] t_before k_before
        r debug reload
        lassign [tmpl_stats] t_after k_after
        assert_equal $t_before $t_after
        assert_equal $k_before $k_after
        assert_equal [r hgetall rdb:k1] {a 1 b 2 c 3}
        assert_equal [r hget rdb:k2 b] 8
        assert_equal [r object encoding rdb:k1] "listpack"
    }

    test {threshold=1: HDEL releases template ref when key is dropped} {
        r flushall
        wait_tmpl_drain
        r hset del:1 a 1 b 2 c 3
        lassign [tmpl_stats] _ k1
        r del del:1
        lassign [tmpl_stats] _ k2
        assert_equal [expr {$k1 - $k2}] 1
    }

    test {threshold=1: AOF rewrite preserves auto-converted hashes} {
        r config set aof-use-rdb-preamble no
        r flushall
        wait_tmpl_drain
        waitForBgrewriteaof r
        r hset aof:1 a 1 b 2 c 3
        r hset aof:2 a 9 b 8 c 7
        lassign [tmpl_stats] t_before k_before
        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof
        lassign [tmpl_stats] t_after k_after
        assert_equal $t_before $t_after
        assert_equal $k_before $k_after
        assert_equal [r hgetall aof:1] {a 1 b 2 c 3}
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
        r himport prepare t5_tpl a b c d
        r himport set t5 t5_tpl 1 2 3 4
        assert_equal [r object encoding t5] template-listpack

        r hset t5 a [string repeat x 100]
        assert_equal [r object encoding t5] template-array
        assert_equal [r hget t5 a] [string repeat x 100]

        r del t5
        r himport discard t5_tpl
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
