set ::slot_prefixes [dict create \
    0 "{06S}" \
    1 "{Qi}" \
    2 "{5L5}" \
    3 "{4Iu}" \
    4 "{4gY}" \
    5 "{460}" \
    6 "{1Y7}" \
    7 "{1LV}" \
    101 "{1j2}" \
    102 "{75V}" \
    103 "{bno}" \
    5462 "{450}"\
    5463 "{4dY}"\
    6000 "{4L7}" \
    6001 "{4YV}" \
    6002 "{0bx}" \
    6003 "{AJ}" \
    6004 "{of}" \
    16383 "{6ZJ}" \
]

# Helper functions
proc get_port {node_id} {
    if {$::tls} {
        return [lindex [R $node_id config get tls-port] 1]
    } else {
        return [lindex [R $node_id config get port] 1]
    }
}

# return the prefix for the given slot
proc slot_prefix {slot} {
    return [dict get $::slot_prefixes $slot]
}

# return a key for the given slot
proc slot_key {slot {suffix ""}} {
    return "[slot_prefix $slot]$suffix"
}

# Populate a slot with keys
# TODO: Consider merging with populate()
proc populate_slot {num args} {
    # Default values
    set prefix "key:"
    set size 3
    set idx 0
    set prints false
    set expires 0
    set slot -1

    # Parse named arguments
    foreach {key value} $args {
        switch -- $key {
            -prefix { set prefix $value }
            -size { set size $value }
            -idx { set idx $value }
            -prints { set prints $value }
            -expires { set expires $value }
            -slot { set slot $value }
            default { error "Unknown option: $key" }
        }
    }

    # If slot is specified, use slot prefix from table
    if {$slot >= 0} {
        if {[dict exists $::slot_prefixes $slot]} {
            set prefix [dict get $::slot_prefixes $slot]
        } else {
            error "Slot $slot not supported in slot_prefixes table, add it manually"
        }
    }

    R $idx deferred 1
    if {$num > 16} {set pipeline 16} else {set pipeline $num}
    set val [string repeat A $size]
    for {set j 0} {$j < $pipeline} {incr j} {
        if {$expires > 0} {
            R $idx set $prefix$j $val ex $expires
        } else {
            R $idx set $prefix$j $val
        }
        if {$prints} {puts $j}
    }
    for {} {$j < $num} {incr j} {
        if {$expires > 0} {
            R $idx set $prefix$j $val ex $expires
        } else {
            R $idx set $prefix$j $val
        }
        R $idx read
        if {$prints} {puts $j}
    }
    for {set j 0} {$j < $pipeline} {incr j} {
        R $idx read
        if {$prints} {puts $j}
    }
    R $idx deferred 0
}

# Return 1 if all instances are idle
proc asm_all_instances_idle {total} {
    for {set i 0} {$i < $total} {incr i} {
        if {[CI $i cluster_slot_migration_active_tasks] != 0} { return 0 }
        if {[CI $i cluster_slot_migration_active_trim_running] != 0} { return 0 }
    }
    return 1
}

# Wait for all ASM tasks to complete in the cluster
proc wait_for_asm_done {} {
    set total_instances [expr {$::cluster_master_nodes + $::cluster_replica_nodes}]

    wait_for_condition 1000 10 {
        [asm_all_instances_idle $total_instances] == 1
    } else {
        # Print the number of active tasks on each instance
        for {set i 0} {$i < $total_instances} {incr i} {
            set migration_count [CI $i cluster_slot_migration_active_tasks]
            set trim_count [CI $i cluster_slot_migration_active_trim_running]
            puts "Instance $i: migration_tasks=$migration_count, trim_tasks=$trim_count"
        }
        fail "ASM tasks did not complete on all instances"
    }
    # wait all nodes to reach the same cluster config after ASM
    wait_for_cluster_propagation
}

proc failover_and_wait_for_done {node_id {failover_arg ""}} {
    set max_attempts 5
    for {set attempt 1} {$attempt <= $max_attempts} {incr attempt} {
        if {$failover_arg eq ""} {
            R $node_id cluster failover
        } else {
            R $node_id cluster failover $failover_arg
        }

        set completed 1
        wait_for_condition 1000 10 {
            [string match "*master*" [R $node_id role]]
        } else {
            set completed 0
        }

        if {$completed} {
            wait_for_cluster_propagation
            return
        }
    }
    fail "Failover did not complete after $max_attempts attempts for node $node_id"
}

proc migration_status {node_id task_id field} {
    set status [R $node_id CLUSTER MIGRATION STATUS ID $task_id]

    # STATUS ID returns single task, so get first element
    if {[llength $status] == 0} {
        return ""
    }

    set task_status [lindex $status 0]
    set field_value ""

    # Parse the key-value pairs in the task
    for {set i 0} {$i < [llength $task_status]} {incr i 2} {
        set key [lindex $task_status $i]
        set value [lindex $task_status [expr $i + 1]]

        if {$key eq $field} {
            set field_value $value
            break
        }
    }

    return $field_value
}

# Setup slot migration test with keys and delay, then start migration
# Returns the task_id for the migration
proc setup_slot_migration_with_delay {src_node dst_node start_slot end_slot {keys 2} {delay 1000000}} {
    # Two keys on the start slot
    populate_slot $keys -idx $src_node -slot $start_slot

    # we set a delay to ensure migration takes time for testing,
    # with default parameters, two keys cost 2s to save
    R $src_node config set rdb-key-save-delay $delay

    # migrate slot range from src_node to dst_node
    set task_id [R $dst_node CLUSTER MIGRATION IMPORT $start_slot $end_slot]
    wait_for_condition 2000 10 {
        [string match {*send-bulk-and-stream*} [migration_status $src_node $task_id state]]
    } else {
        fail "ASM task did not start"
    }

    return $task_id
}

# Skip most of the tests when running under valgrind since it is hard to
# stabilize tests under valgrind.
if {!$::valgrind} {
start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-node-timeout 60000 cluster-allow-replica-migration no loglevel debug}} {
    test "Test CLUSTER MIGRATION IMPORT input validation" {
        # invalid arguments
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION IMPORT}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION IMPORT 100}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION IMPORT 100 200 300}
        assert_error {*unknown argument*} {R 0 CLUSTER MIGRATION UNKNOWN 1 2}

        # invalid slot range
        assert_error {*greater than end slot number*} {R 0 CLUSTER MIGRATION IMPORT 200 100}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT 17000 18000}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT 14000 18000}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT 0 16384}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT 0 -1}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT -1 2}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT -2 -1}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT 10 a}
        assert_error {*out of range slot*} {R 0 CLUSTER MIGRATION IMPORT sd sd}
        assert_error {*already the owner of the slot*} {R 0 CLUSTER MIGRATION IMPORT 100 200}
    }

    test "Test CLUSTER MIGRATION CANCEL input validation" {
        # invalid arguments
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION CANCEL}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION CANCEL ID}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION CANCEL ID 12345 EXTRAARG}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION CANCEL ALL EXTRAARG}
        assert_error {*unknown argument*} {R 0 CLUSTER MIGRATION CANCEL UNKNOWNARG}
        assert_error {*unknown argument*} {R 0 CLUSTER MIGRATION CANCEL abc def}
        # empty string id should not cancel any task
        assert_equal 0 [R 0 CLUSTER MIGRATION CANCEL ID ""]
    }

    test "Test CLUSTER MIGRATION STATUS input validation" {
        # invalid arguments
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION STATUS}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION STATUS ID}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION STATUS ID id EXTRAARG}
        assert_error {*wrong number of arguments*} {R 0 CLUSTER MIGRATION STATUS ALL EXTRAARG}
        assert_error {*unknown argument*} {R 0 CLUSTER MIGRATION STATUS ABC DEF}
        assert_error {*unknown argument*} {R 0 CLUSTER MIGRATION STATUS UNKNOWNARG}
        # empty string id should not list any task
        assert_equal {} [R 0 CLUSTER MIGRATION STATUS ID ""]
    }

    test "Test TRIMSLOTS input validation" {
        # Wrong number of arguments
        assert_error {*wrong number of arguments*} {R 0 TRIMSLOTS}
        assert_error {*wrong number of arguments*} {R 0 TRIMSLOTS RANGES}
        assert_error {*wrong number of arguments*} {R 0 TRIMSLOTS RANGES 1}
        assert_error {*wrong number of arguments*} {R 0 TRIMSLOTS RANGES 2 100}
        assert_error {*wrong number of arguments*} {R 0 TRIMSLOTS RANGES 17000 1}
        assert_error {*wrong number of arguments*} {R 0 TRIMSLOTS RANGES abc}

        # Missing ranges argument
        assert_error {*missing ranges argument*} {R 0 TRIMSLOTS UNKNOWN 1 100 200}

        # Invalid number of ranges
        assert_error {*invalid number of ranges*} {R 0 TRIMSLOTS RANGES 0 1 1}
        assert_error {*invalid number of ranges*} {R 0 TRIMSLOTS RANGES -1 2 2}
        assert_error {*invalid number of ranges*} {R 0 TRIMSLOTS RANGES 17000 1 2}
        assert_error {*invalid number of ranges*} {R 0 TRIMSLOTS RANGES 2 100 200 300}

        # Invalid slot numbers
        assert_error {*out of range slot*} {R 0 TRIMSLOTS RANGES 1 -1 0}
        assert_error {*out of range slot*} {R 0 TRIMSLOTS RANGES 1 -2 -1}
        assert_error {*out of range slot*} {R 0 TRIMSLOTS RANGES 1 0 16384}
        assert_error {*out of range slot*} {R 0 TRIMSLOTS RANGES 1 abc def}
        assert_error {*out of range slot*} {R 0 TRIMSLOTS RANGES 1 100 abc}

        # Start slot greater than end slot
        assert_error {*greater than end slot number*} {R 0 TRIMSLOTS RANGES 1 200 100}
    }

    test "Test IMPORT not allowed on replica" {
        assert_error {* not allowed on replica*} {R 4 CLUSTER MIGRATION IMPORT 100 200}
    }

    test "Test IMPORT not allowed during manual migration" {
        set dst_id [R 1 CLUSTER MYID]

        # Set a slot to IMPORTING
        R 0 CLUSTER SETSLOT 15000 IMPORTING $dst_id
        assert_error {*must be STABLE to start*slot migration*} {R 0 CLUSTER MIGRATION IMPORT 100 200}
        # Revert the change
        R 0 CLUSTER SETSLOT 15000 STABLE

        # Same test with setting a slot to MIGRATING
        R 0 CLUSTER SETSLOT 5000 MIGRATING $dst_id
        assert_error {*must be STABLE to start*slot migration*} {R 0 CLUSTER MIGRATION IMPORT 100 200}
        # Revert the change
        R 0 CLUSTER SETSLOT 5000 STABLE
    }

    test "Test IMPORT not allowed if the node is already the owner" {
        assert_error {*already the owner of the slot*} {R 0 CLUSTER MIGRATION IMPORT 100 100}
    }

    test "Test IMPORT not allowed for a slot without an owner" {
        # Slot will have no owner
        R 0 CLUSTER DELSLOTS 5000

        assert_error {*slot has no owner: 5000*} {R 0 CLUSTER MIGRATION IMPORT 5000 5000}

        # Revert the change
        R 0 CLUSTER ADDSLOTS 5000
    }

    test "Test IMPORT not allowed if slot ranges belong to different nodes" {
        assert_error {*slots belong to different source nodes*} {R 0 CLUSTER MIGRATION IMPORT 7000 15000}
        assert_error {*slots belong to different source nodes*} {R 0 CLUSTER MIGRATION IMPORT 7000 8000 14000 15000}
    }

    test "Test IMPORT not allowed if slot is given multiple times" {
        assert_error {*Slot*specified multiple times*} {R 0 CLUSTER MIGRATION IMPORT 7000 8000 8000 9000}
        assert_error {*Slot*specified multiple times*} {R 0 CLUSTER MIGRATION IMPORT 7000 8000 7900 9000}
    }

    test "Test CLUSTER MIGRATION STATUS ALL lists all tasks" {
        # Create 3 completed tasks
        R 0 CLUSTER MIGRATION IMPORT 7000 7001
        wait_for_asm_done
        R 0 CLUSTER MIGRATION IMPORT 7002 7003
        wait_for_asm_done
        R 0 CLUSTER MIGRATION IMPORT 7004 7005
        wait_for_asm_done

        # Get node IDs for verification
        set node0_id [R 0 cluster myid]
        set node1_id [R 1 cluster myid]

        # Verify CLUSTER MIGRATION STATUS ALL reply from both nodes
        foreach node_idx {0 1} {
            set tasks [R $node_idx CLUSTER MIGRATION STATUS ALL]
            assert_equal 3 [llength $tasks]

            for {set i 0} {$i < 3} {incr i} {
                set task [lindex $tasks $i]

                # Verify field order
                set expected_fields {id slots source dest operation state
                                    last_error retries create_time start_time
                                    end_time write_pause_ms}
                for {set j 0} {$j < [llength $expected_fields]} {incr j} {
                    set expected_field [lindex $expected_fields $j]
                    set actual_field [lindex $task [expr $j * 2]]
                    assert_equal $expected_field $actual_field
                }

                # Verify basic fields
                assert_equal "completed" [dict get $task state]
                assert_equal "" [dict get $task last_error]
                assert_equal 0 [dict get $task retries]
                assert {[dict get $task write_pause_ms] >= 0}

                # Verify operation based on node
                if {$node_idx == 0} {
                    assert_equal "import" [dict get $task operation]
                } else {
                    assert_equal "migrate" [dict get $task operation]
                }

                # Verify node IDs (all tasks: node1 -> node0)
                assert_equal $node1_id [dict get $task source]
                assert_equal $node0_id [dict get $task dest]

                # Verify timestamps exist and are reasonable
                set create_time [dict get $task create_time]
                set start_time [dict get $task start_time]
                set end_time [dict get $task end_time]
                assert {$create_time > 0}
                assert {$start_time >= $create_time}
                assert {$end_time >= $start_time}

                # Verify specific slot ranges for each task
                set slots [dict get $task slots]
                if {$i == 0} {
                    assert_equal "7004-7005" $slots
                } elseif {$i == 1} {
                    assert_equal "7002-7003" $slots
                } elseif {$i == 2} {
                    assert_equal "7000-7001" $slots
                }
            }
        }

        # cleanup
        R 1 CLUSTER MIGRATION IMPORT 7000 7005
        wait_for_asm_done
    }

    test "Test IMPORT not allowed if there is an overlapping import" {
        # Let slot migration take long time, so that we can test overlapping import
        R 1 config set rdb-key-save-delay 1000000
        R 1 set tag22273 tag22273 ;# slot hash is 7000
        R 1 set tag9283 tag9283 ;# slot hash is 8000

        set task_id [R 0 CLUSTER MIGRATION IMPORT 7000 8000]
        assert_error {*overlapping import exists*} {R 0 CLUSTER MIGRATION IMPORT 8000 9000}
        assert_error {*overlapping import exists*} {R 0 CLUSTER MIGRATION IMPORT 7500 8500}
        assert_error {*overlapping import exists*} {R 0 CLUSTER MIGRATION IMPORT 6000 7000}
        assert_error {*overlapping import exists*} {R 0 CLUSTER MIGRATION IMPORT 6500 7500}

        wait_for_condition 1000 50 {
            [string match {*completed*} [migration_status 0 $task_id state]] &&
            [string match {*completed*} [migration_status 1 $task_id state]]
        } else {
            fail "ASM task did not start"
        }
        assert_equal "tag22273" [R 0 get tag22273]
        assert_equal "tag9283" [R 0 get tag9283]
        R 1 config set rdb-key-save-delay 0

        # revert the migration
        R 1 CLUSTER MIGRATION IMPORT 7000 8000
        wait_for_asm_done
    }

    test "Simple slot migration with write load" {
        # Perform slot migration while traffic is on and verify data consistency.
        # Trimming is disabled on source nodes so, we can compare the dbs after
        # migration via DEBUG DIGEST to ensure no data loss during migration.
        # Steps:
        # 1. Disable trimming on both nodes
        # 2. Populate slot 0 on node-0 and slot 6000 on node-1
        # 2. Start write traffic on both nodes
        # 3. Migrate slot 0 from node-0 to node-1
        # 4. Migrate slot 6000 from node-1 to node-0
        # 5. Stop write traffic, verify db's are identical.

        set prev_config [lindex [R 0 config get cluster-slot-migration-handoff-max-lag-bytes] 1]
        R 0 config set cluster-slot-migration-handoff-max-lag-bytes 10mb
        R 1 config set cluster-slot-migration-handoff-max-lag-bytes 10mb

        R 0 flushall
        R 0 debug asm-trim-method none
        populate_slot 10000 -idx 0 -slot 0

        R 1 flushall
        R 1 debug asm-trim-method none
        populate_slot 10000 -idx 1 -slot 6000

        # Start write traffic on node-0
        # Throws -MOVED error once asm is completed, catch block will ignore it.
        catch {
            # Start the slot 0 write load on the R 0
            set port [get_port 0]
            set key [slot_key 0 mykey]
            set load_handle0 [start_write_load "127.0.0.1" $port 100 $key]
        }

        # Start write traffic on node-1
        # Throws -MOVED error once asm is completed, catch block will ignore it.
        catch {
            # Start the slot 6000 write load on the R 1
            set port [get_port 1]
            set key [slot_key 6000 mykey]
            set load_handle1 [start_write_load "127.0.0.1" $port 100 $key]
        }

        # Migrate keys
        R 1 CLUSTER MIGRATION IMPORT 0 100
        wait_for_asm_done
        R 0 CLUSTER MIGRATION IMPORT 6000 6100
        wait_for_asm_done

        stop_write_load $load_handle0
        stop_write_load $load_handle1

        # verify data
        assert_morethan [R 0 dbsize] 0
        assert_equal [R 0 debug digest] [R 1 debug digest]

        # cleanup
        R 0 config set cluster-slot-migration-handoff-max-lag-bytes $prev_config
        R 0 debug asm-trim-method default
        R 0 flushall
        R 1 config set cluster-slot-migration-handoff-max-lag-bytes $prev_config
        R 1 debug asm-trim-method default
        R 1 flushall

        R 1 CLUSTER MIGRATION IMPORT 6000 6100
        wait_for_asm_done
    }
}
}
