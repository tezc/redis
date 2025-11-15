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
