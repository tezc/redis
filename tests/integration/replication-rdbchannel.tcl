# Returns either main or rdbchannel client id
# Assumes there is one replica with two channels
proc get_replica_client_id {master rdbchannel} {
    set input [$master client list type replica]

    foreach line [split $input "\n"] {
        if {[regexp {id=(\d+).*flags=(\S+)} $line match id flags]} {
            if {$rdbchannel == "yes"} {
                # rdbchannel will have C flag
                if {[string match *C* $flags]} {
                    return $id
                }
            } else {
                return $id
            }
        }
    }

    error "Replica not found"
}

start_server {tags {"repl external:skip"}} {
    set replica1 [srv 0 client]

    start_server {} {
        set replica2 [srv 0 client]

        start_server {} {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            $master config set repl-diskless-sync yes
            $master config set repl-rdb-channel yes
            populate 1000 master 10

            test "Test replication with multiple replicas (rdbchannel enabled on both)" {
                $replica1 config set repl-rdb-channel yes
                $replica1 replicaof $master_host $master_port

                $replica2 config set repl-rdb-channel yes
                $replica2 replicaof $master_host $master_port

                wait_replica_online $master 0
                wait_replica_online $master 1

                $master set x 1

                # Wait until replicas catch master
                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2

                # Verify db's are identical
                assert_morethan [$master dbsize] 0
                assert_equal [$master get x] 1
                assert_equal [$master debug digest] [$replica1 debug digest]
                assert_equal [$master debug digest] [$replica2 debug digest]
            }

            test "Test replication with multiple replicas (rdbchannel enabled on one of them)" {
                # Allow both replicas to ask for sync
                $master config set repl-diskless-sync-delay 5

                $replica1 replicaof no one
                $replica2 replicaof no one
                $replica1 config set repl-rdb-channel yes
                $replica2 config set repl-rdb-channel no

                set prev_forks [s 0 total_forks]
                $master set x 2

                # There will be two forks subsequently, one for rdbchannel
                # replica another for the replica without rdbchannel config.
                $replica1 replicaof $master_host $master_port
                $replica2 replicaof $master_host $master_port

                set res [wait_for_log_messages 0 {"*Starting BGSAVE* replicas sockets (rdb-channel).*"} 0 2000 10]
                set loglines [lindex $res 1]
                wait_for_log_messages 0 {"*Starting BGSAVE* replicas sockets.*"} $loglines 2000 10

                wait_replica_online $master 0
                wait_replica_online $master 1

                # Verify two new forks.
                assert_equal [s 0 total_forks] [expr $prev_forks + 2]

                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2

                # Verify db's are identical
                assert_equal [$replica1 get x] 2
                assert_equal [$replica2 get x] 2
                assert_equal [$master debug digest] [$replica1 debug digest]
                assert_equal [$master debug digest] [$replica2 debug digest]
            }

            test "Test rdbchannel is not used if repl-diskless-sync config is disabled on master" {
                $replica1 replicaof no one
                $replica2 replicaof no one

                $master config set repl-diskless-sync-delay 0
                $master config set repl-diskless-sync no

                $master set x 3
                set prev_partial_ok [status $master sync_partial_ok]

                $replica1 replicaof $master_host $master_port

                # Verify log message does not mention rdbchannel
                wait_for_log_messages 0 {"*Starting BGSAVE for SYNC with target: disk.*"} 0 2000 1

                wait_replica_online $master 0
                wait_for_ofs_sync $master $replica1

                # Verify db's are identical
                assert_equal [$replica1 get x] 3
                assert_equal [$master debug digest] [$replica1 debug digest]

                # rdbchannel replication would have increased psync count.
                assert_equal [status $master sync_partial_ok] $prev_partial_ok
            }
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]

    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        $master config set repl-rdb-channel yes
        $replica config set repl-rdb-channel yes

        test "Test master memory does not increase during replication" {
            # Put some delay to rdb generation. If master doesn't forward
            # incoming traffic to replica, master's replication buffer will grow
            $master config set rdb-key-save-delay 200
            $master config set repl-backlog-size 5mb
            populate 10000 master 10000

            # Start write traffic
            set load_handle [start_write_load $master_host $master_port 100 "key1"]
            set prev_used [s 0 used_memory]

            $replica replicaof $master_host $master_port
            set backlog_size [lindex [$master config get repl-backlog-size] 1]

            # Verify used_memory stays low
            set max_retry 1000
            set prev_buf_size 0
            while {$max_retry} {
                assert_lessthan [expr [s 0 used_memory] - $prev_used] 20000000
                assert_lessthan_equal [s 0 mem_total_replication_buffers] [expr {$backlog_size + 1000000}]

                # Check replica state
                if {[string match *slave0*state=online* [$master info]] &&
                    [s -1 master_link_status] == "up"} {
                    break
                } else {
                    incr max_retry -1
                    after 10
                }
            }
            if {$max_retry == 0} {
                error "assertion:Replica not in sync after 10 seconds"
            }

            stop_write_load $load_handle
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]
    set replica_pid [srv 0 pid]

    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set backlog_size [expr {10 ** 5}]

        $master config set repl-diskless-sync yes
        $master config set repl-rdb-channel yes
        $master config set repl-backlog-size $backlog_size
        $master config set loglevel debug
        $master config set rdb-key-save-delay 200

        $replica config set repl-rdb-channel yes
        $replica config set loglevel debug

        test "Test master backlog can grow beyond its configured limit" {
            # Simulate slow consumer. If replica does not consume fast enough,
            # master's backlog will grow.

            # Populate db with some keys, delivery will take some time.
            populate 10000 master 10000

            # Pause replica before psync, so replica will not consume backlog
            $replica debug repl-pause before-main-conn-psync

            # Start write traffic
            set load_handle [start_write_load $master_host $master_port 100 "key"]
            $replica replicaof $master_host $master_port

            # Verify repl backlog can grow
            wait_for_condition 1000 20 {
                [s 0 mem_total_replication_buffers] > [expr {3 * $backlog_size}]
            } else {
                fail "Master should allow backlog to grow beyond its limits during replication"
            }

            # resume replica
            resume_process $replica_pid

            stop_write_load $load_handle

            # Verify recovery. Wait until replica catches up
            wait_replica_online $master 0 100 1000
            wait_for_ofs_sync $master $replica

            # Verify db's are identical
            assert_morethan [$master dbsize] 0
            assert_equal [$master debug digest] [$replica debug digest]
        }
   }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]

    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        $master config set repl-rdb-channel yes
        $replica config set repl-rdb-channel yes

        test "Test replication stream buffer becomes full on replica" {
            # For replication stream accumulation, replica inherits slave output
            # buffer limit as the size limit. In this test, we create traffic to
            # fill the buffer fully. Once the limit is reached, accumulation
            # will stop. This is not a failure scenario though. From that point,
            # further accumulation may occur on master side. Replication should
            # be completed successfully.

            # Create some artificial delay for rdb delivery and load. We'll
            # generate some traffic meanwhile to fill replication buffer.
            $master config set rdb-key-save-delay 1000
            $replica config set key-load-delay 1000
            $replica config set client-output-buffer-limit "replica 64kb 64kb 0"
            populate 2000 master 1

            set prev_sync_full [s 0 sync_full]
            $replica replicaof $master_host $master_port

            # Wait for replica to establish psync using main channel
            wait_for_condition 500 1000 {
                [string match "*state=bg_rdb_transfer*" [s 0 slave0]]
            } else {
                fail "replica didn't start sync"
            }

            # Create some traffic on replication stream
            populate 100 master 100000

            # Wait for replica's buffer limit reached
            wait_for_log_messages -1 {"*Replication buffer limit has been reached*"} 0 1000 10

            # Speed up loading
            $replica config set key-load-delay 0

            # Wait until sync is successful
            wait_for_condition 200 200 {
                [status $master master_repl_offset] eq [status $replica master_repl_offset] &&
                [status $master master_repl_offset] eq [status $replica slave_repl_offset]
            } else {
                fail "replica offsets didn't match in time"
            }

            # Verify sync was not interrupted.
            assert_equal [s 0 sync_full] [expr $prev_sync_full + 1]

            # Verify db's are identical
            assert_morethan [$master dbsize] 0
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica1 [srv 0 client]
    set replica1_pid  [srv 0 pid]

    start_server {} {
        set replica2 [srv 0 client]
        set replica2_pid  [srv 0 pid]

        start_server {} {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            test "Test replicas are attached to backlog" {
                # Once rdb delivery starts, rdb channel slave keeps a reference
                # to backlog on master, so main channel psync attempt will be
                # successful. In this test, we verify replicas rdbchannel
                # connections are attached to backlog successfully.
                $master config set repl-diskless-sync yes
                $master config set repl-rdb-channel yes
                $master config set loglevel debug

                $replica1 config set repl-rdb-channel yes
                $replica2 config set repl-rdb-channel yes

                # Replicas will sleep before establishing psync. On master,
                # their rdbchannel connection will maintain a reference to the
                # backlog.
                $replica1 debug repl-pause before-main-conn-psync
                $replica2 debug repl-pause before-main-conn-psync

                # Populate db
                populate 1000 master 10

                set loglines [count_log_lines -1]
                set load_handle [start_write_load $master_host $master_port 20]
                $replica1 replicaof $master_host $master_port

                # For the first replica, there is no backlog on the master.
                # It will be lazily attached to backlog once the first backlog
                # object is created.
                wait_for_log_messages 0 {"*Added rdb replica*to the psync waiting list*tail = 0*"} $loglines 2000 1

                # Wait until first backlog node is created
                set res [wait_for_log_messages 0 {"*Attached replica rdb client*"} $loglines 2000 1]
                set loglines [lindex $res 1]
                incr $loglines

                # Second replica will be attached to backlog tail.
                $replica2 replicaof $master_host $master_port
                wait_for_log_messages 0 {"*Added rdb replica*to the psync waiting list*tail = 1*"} $loglines 2000 1

                # Resume replicas and verify the recovery
                resume_process $replica1_pid
                resume_process $replica2_pid

                stop_write_load $load_handle

                # Wait until replication is completed
                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2

                assert_equal [s 0 replicas_waiting_psync] 0

                # Verify db's are identical
                assert_equal [$master debug digest] [$replica1 debug digest]
                assert_equal [$master debug digest] [$replica2 debug digest]
            }
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]
    set replica_pid [srv 0 pid]

    start_server {} {
        set master [srv 0 client]
        set master_pid  [srv 0 pid]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        # Create small enough db to be loaded before replica establishes psync
        $master set key1 val1

        $master config set repl-diskless-sync yes
        $master config set repl-rdb-channel yes
        $master config set loglevel debug

        $replica config set repl-rdb-channel yes
        $replica config set loglevel debug

        test "Test rdb is loaded before main channel establishes psync (success scenario)" {
            # Steps:
            #  1. Replica initiates synchronization via RDB channel.
            #  2. Master's main process is suspended. RDB is delivered by the fork.
            #  3. Replica completes RDB loading, closes RDB channel.
            #  4. Replica pauses before establishing psync.
            #  5. Master resumes operation and detects closed RDB channel.
            #  6. Replica resumes operation and establishes pysnc.
            #
            #  We expect master to keep rdbchannel reference to the backlog so
            #  psync at step-6 can be successful.

            # Pause master main process after fork
            $master debug repl-pause after-fork

            # Give replica five second grace period before disconnection
            $master debug delay-rdb-client-free 5
            $replica config set repl-timeout 10

            set loglines [count_log_lines -1]

            # Start some write traffic. If master does not prevent backlog
            # trimming, replicas psync attempt will fail.
            set load_handle [start_write_load $master_host $master_port 20]

            $replica replicaof $master_host $master_port

            # Wait until replica loads rdb
            wait_for_log_messages -1 {"*Done loading RDB*"} $loglines 1000 10

            # Pause replica and resume master
            pause_process $replica_pid
            resume_process $master_pid

            # Master will wait until replica main channel establishes psync
            wait_for_log_messages 0 {"*Postponed RDB client*free*"} 0 1000 10
            wait_for_condition 50 100 {
                [s 0 replicas_waiting_psync] == 1
            } else {
                fail "Master freed RDB client before psync was established"
            }

            # Resume replica and verify it establishes psync
            resume_process $replica_pid
            wait_for_log_messages -1 {"*Main channel established psync after rdb load*"} 0 1000 10

            wait_replica_online $master 0
            wait_for_condition 50 100 {
                [s 0 replicas_waiting_psync] == 0
            } else {
                fail "Master did not delete rdb client from the psync list"
            }

            stop_write_load $load_handle

            # Verify system is operational.
            $master set x 1
            $master set y 2
            wait_replica_online $master 0 200 500
            wait_for_ofs_sync $master $replica

            # Verify db's are identical after sync
            assert_morethan [$master dbsize] 0
            assert_equal [$replica get x] 1
            assert_equal [$replica get y] 2
            wait_for_ofs_sync $master $replica
            assert_equal [$master dbsize] [$replica dbsize]
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]
    set replica_pid [srv 0 pid]

    start_server {} {
        set master [srv 0 client]
        set master_pid  [srv 0 pid]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        # Create small enough db to be loaded before replica establishes psync
        $master set key1 val1

        $master config set repl-diskless-sync yes
        $master config set repl-rdb-channel yes
        $master config set loglevel debug

        $replica config set repl-rdb-channel yes
        $replica config set loglevel debug

        test "Test rdb is loaded before main channel establishes psync (fail scenario)" {
             # Steps:
             #  1. Replica initiates synchronization via RDB channel.
             #  2. Master's main process is suspended. RDB is delivered by the fork.
             #  3. Replica completes RDB loading, closes RDB channel.
             #  4. Replica pauses before establishing psync.
             #  5. Master resumes operation and detects closed RDB channel.
             #  6. Replica is too late to establish psync, master drops rdbchannel
             #     client and its backlog reference.
             #  7. Replica resumes operation and pysnc will fail.

             # Pause master main process after fork
             $master debug repl-pause after-fork

             # Replica has three seconds to establish psync after rdb delivery
             # is completed.
             $master debug delay-rdb-client-free 3

             set loglines [count_log_lines -1]
             set load_handle [start_write_load $master_host $master_port 20]

             $replica replicaof $master_host $master_port

             # Wait until replica loads rdb
             wait_for_log_messages -1 {"*Done loading RDB*"} $loglines 1000 10

             # Pause replica and resume master
             pause_process $replica_pid
             resume_process $master_pid

             wait_for_condition 50 100 {
                 [s 0 replicas_waiting_psync] == 1
             } else {
                 fail "Master deleted RDB client before psync was established"
             }

             # Master will free rdbchannel client and drop its reference to the
             # backlog. Replicas psync attempt will fail.
             set res [wait_for_log_messages 0 {"*Replica main channel failed to establish PSYNC within the grace period*"} 0 1000 10]
             wait_for_condition 50 100 {
                 [s 0 replicas_waiting_psync] == 0
             } else {
                 fail "Master did not delete waiting psync replica after grace period"
             }

             $master debug repl-pause clear

             # Create some traffic, backlog will move on to some other offset
             populate 200 master 10000

             # Sync should fail once the replica ask for PSYNC using main channel
             set prev_partial_err [s 0 sync_partial_err]
             resume_process $replica_pid

             wait_for_condition 50 200 {
                 [s 0 sync_partial_err] > $prev_partial_err
             } else {
                 puts "sync_partial_err: [s 0 sync_partial_err],
                       prev_partial_err: $prev_partial_err"
                 fail "Replica psync request should have failed"
             }

             stop_write_load $load_handle

            # Verify system is operational.
            $master set x 1
            $master set y 2
            wait_replica_online $master 0 200 500
            wait_for_ofs_sync $master $replica

            # Verify db's are identical after sync
            assert_morethan [$master dbsize] 0
            assert_equal [$replica get x] 1
            assert_equal [$replica get y] 2
            wait_for_ofs_sync $master $replica
            assert_equal [$master dbsize] [$replica dbsize]
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master_pid  [srv 0 pid]
    set loglines [count_log_lines 0]

    $master config set repl-diskless-sync yes
    $master config set repl-rdb-channel yes
    $master config set repl-backlog-size 1mb
    $master config set client-output-buffer-limit "replica 100k 0 0"
    $master config set loglevel debug
    $master config set repl-diskless-sync-delay 3

    start_server {} {
        set replica [srv 0 client]
        set replica_pid [srv 0 pid]

        $replica config set repl-rdb-channel yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10

        test "Test master disconnects replica when output buffer limit is reached (after rdb delivery)" {
            # Steps:
            #  1. Replica initiates synchronization via RDB channel.
            #  2. Master's main process is suspended. RDB is delivered by the fork.
            #  3. Replica completes RDB loading, closes RDB channel.
            #  4. Replica pauses before establishing psync.
            #  5. Master resumes operation.
            #  6. As replica hasn't established psync yet, master will accumulate backlog.
            #  7. When client output buffer limit is reached, master will disconnect replica.

            # Pause master main process after fork
            $master debug repl-pause after-fork

            $replica replicaof $master_host $master_port
            # Wait until replica loads RDB
            wait_for_log_messages 0 {"*Done loading RDB*"} 0 1000 10

            # At this point rdb is loaded but psync hasn't been established yet.
            # Pause the replica so the master main process will wake up while
            # the replica is unresponsive. We expect the main process to fill
            # the client output buffer and disconnect the replica.
            pause_process $replica_pid
            resume_process $master_pid

            # Disable pause flag
            $master debug repl-pause clear

            # Generate some traffic for backlog ~2mb
            populate 200 master 10000 -1

            set res [wait_for_log_messages -1 {"*Client * closed * for overcoming of output buffer limits.*"} $loglines 1000 10]
            set loglines [lindex $res 1]

            # Verify master deletes replica from psync waiting list.
            wait_for_condition 50 100 {
                [s -1 replicas_waiting_psync] == 0
            } else {
                fail "Master did not delete replica from psync waiting list"
            }

            # Resume replica and verify it fails to establish psync.
            resume_process $replica_pid
            set res [wait_for_log_messages -1 {"*Unable to partial resync with replica * for lack of backlog*"} $loglines 2000 100]
            set loglines [lindex $res 1]

            # Wait until replica catches up
            wait_replica_online $master 0 1000 100
        }

        test "Test master disconnects replica when output buffer limit is reached (during rdb delivery)" {
            # Steps:
            #  1. Replica initiates synchronization via RDB channel.
            #  2. Master starts delivering RDB.
            #  3. Replica pauses before establishing psync.
            #  4. Backlog grows on master as replica does not consume it.
            #  5. When client output buffer limit is reached, master will disconnect replica.

            $replica replicaof no one
            $replica debug repl-pause before-main-conn-psync

            # Set master with a slow rdb generation, so that we can easily
            # intercept loading 10ms per key, with 20000 keys is 200 seconds
            $master config set rdb-key-save-delay 10000
            populate 20000 master 100

            set client_disconnected [s -1 client_output_buffer_limit_disconnections]
            $replica replicaof $master_host $master_port

            # RDB delivery starts
            wait_for_condition 50 100 {
                [s -1 rdb_bgsave_in_progress] == 1
            } else {
                fail "Sync did not start"
            }

            # Create some traffic on backlog ~2mb
            populate 200 master 10000 -1

            # Verify master kills replica connection.
            wait_for_condition 50 10 {
                [s -1 client_output_buffer_limit_disconnections] == $client_disconnected + 1
            } else {
                fail "Master should disconnect replica"
            }
            set res [wait_for_log_messages -1 {"*Client * closed * for overcoming of output buffer limits.*"} $loglines 1000 10]
            set loglines [lindex $res 1]

            wait_for_condition 50 100 {
                [s -1 replicas_waiting_psync] == 0
            } else {
                fail "Master did not delete replica from psync waiting list"
            }

            # Resume replica
            resume_process $replica_pid

            wait_for_log_messages -1 {"*Unable to partial resync with replica * for lack of backlog*"} $loglines 2000 10

            # Speed up replication
            $replica debug repl-pause clear
            $master config set repl-diskless-sync-delay 0
            $master config set rdb-key-save-delay 0
        }

        test "Test replication recovers after output buffer failures" {
            # Verify system is operational
            $master set x 1

            # Wait until replica catches up
            wait_replica_online $master 0 1000 100
            wait_for_ofs_sync $master $replica

            # Verify db's are identical
            assert_morethan [$master dbsize] 0
            assert_equal [$replica get x] 1
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    $master config set repl-diskless-sync yes
    $master config set repl-rdb-channel yes
    $master config set rdb-key-save-delay 300
    $master config set client-output-buffer-limit "replica 0 0 0"
    $master config set repl-diskless-sync-delay 4
    $master config set loglevel debug

    populate 10000 master 1

    start_server {} {
        set replica1 [srv 0 client]
        $replica1 config set repl-rdb-channel yes
        $replica1 config set loglevel debug

        start_server {} {
            set replica2 [srv 0 client]
            $replica2 config set repl-rdb-channel yes
            $replica2 config set loglevel debug

            set load_handle [start_write_load $master_host $master_port 100 "key"]

            test "Test master continues RDB delivery if not all replicas are dropped" {
                $replica1 replicaof $master_host $master_port
                $replica2 replicaof $master_host $master_port

                wait_for_condition 50 100 {
                    [s -2 rdb_bgsave_in_progress] == 1
                } else {
                    fail "Sync did not start"
                }

                # Wait for both replicas main conns to establish psync
                wait_for_condition 500 100 {
                    [s -2 sync_partial_ok] == 2
                } else {
                    fail "Replicas didn't establish psync:
                          sync_partial_ok: [s -2 sync_partial_ok]"
                }

                # kill one of the replicas
                catch {$replica1 shutdown nosave}

                # Wait until replica completes full sync
                # Verify there is no other full sync attempt
                wait_for_condition 50 1000 {
                    [s 0 master_link_status] == "up" &&
                    [s -2 sync_full] == 2 &&
                    [s -2 sync_partial_ok] == 2 &&
                    [s -2 connected_slaves] == 1
                } else {
                    fail "Sync session did not continue
                          master_link_status: [s 0 master_link_status]
                          sync_full:[s -2 sync_full]
                          sync_partial_ok:[s -2 sync_partial_ok]
                          connected_slaves: [s -2 connected_slaves]"
                }
            }

            test "Test master aborts rdb delivery if all replicas are dropped" {
                $replica2 replicaof no one

                # Start replicaof
                set cur_psync [status $master sync_partial_ok]
                $replica2 replicaof $master_host $master_port

                wait_for_condition 50 1000 {
                    [s -2 rdb_bgsave_in_progress] == 1
                } else {
                    fail "Sync did not start"
                }

                # Wait replica main connection to establish psync
                wait_for_condition 50 1000 {
                    [s -2 sync_partial_ok] == $cur_psync + 1
                } else {
                    fail "No new psync, sync_partial_ok: [s -2 sync_partial_ok]"
                }

                set loglines [count_log_lines -2]

                # kill replica
                catch {$replica2 shutdown nosave}

                # Verify master aborts rdb save
                wait_for_condition 50 1000 {
                    [s -2 rdb_bgsave_in_progress] == 0 &&
                    [s -2 connected_slaves] == 0
                } else {
                    fail "Master should abort the sync
                          rdb_bgsave_in_progress:[s -2 rdb_bgsave_in_progress]
                          connected_slaves: [s -2 connected_slaves]"
                }

                wait_for_log_messages -2 {"*Background RDB transfer error*"} $loglines 1000 10
            }

            stop_write_load $load_handle
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    $master config set repl-diskless-sync yes
    $master config set repl-rdb-channel yes
    $master config set loglevel debug
    $master config set rdb-key-save-delay 1000

    populate 3000 prefix1 1
    populate 100 prefix2 100000

    start_server {} {
        set replica [srv 0 client]
        set replica_pid [srv 0 pid]

        $replica config set repl-rdb-channel yes
        $replica config set loglevel debug
        $replica config set repl-timeout 10

        set load_handle [start_write_load $master_host $master_port 100 "key"]

        test "Test replica recovers when rdb channel connection is killed" {
            $replica replicaof $master_host $master_port

            # Wait for sync session to start
            wait_for_condition 500 200 {
                [string match "*state=bg_rdb_transfer*" [s -1 slave0]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't start sync session in time"
            }

            set loglines [count_log_lines -1]

            # There will be two client ids for the two channels. We expect first
            # one to be main-channel and the second one to be rdbchannel as
            # rdbchannel connection is established later. We kill the other id
            # in the next test. So, even if the order changes later, we'll be
            # killing both channels.
            set id [get_replica_client_id $master yes]
            $master client kill id $id

            # Wait for master to abort the sync
            wait_for_condition 500 100 {
                [s -1 replicas_waiting_psync] == 0
            } else {
                fail "Master did not delete replica from psync waiting list"
            }
            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 10

            # Verify master rejects set-rdb-client-id after connection is killed
            assert_error {*Unrecognized*} {$master replconf set-rdb-client-id $id}

            # Replica should retry
            wait_for_condition 500 200 {
                [string match "*state=bg_rdb_transfer*" [s -1 slave0]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't retry after connection close"
            }
        }

        test "Test replica recovers when main channel connection is killed" {
            set loglines [count_log_lines -1]

            # There will be two client ids for the two channels. We expect first
            # one to be main-channel and the second one to be rdbchannel as
            # rdbchannel connection is established later. We kill the other id
            # in the previous test. So, even if the order changes later, we'll
            # be killing both channels.
            set id [get_replica_client_id $master yes]
            $master client kill id $id

            # Wait for master to abort the sync
            wait_for_condition 50 1000 {
                [s -1 replicas_waiting_psync] == 0
            } else {
                fail "replicas_waiting_psync did not become zero"
            }

            wait_for_log_messages -1 {"*Background RDB transfer error*"} $loglines 1000 20

            # Replica should retry
            wait_for_condition 500 2000 {
                [string match "*state=bg_rdb_transfer*" [s -1 slave0]] &&
                [s -1 rdb_bgsave_in_progress] eq 1
            } else {
                fail "replica didn't retry after connection close"
            }
        }

        stop_write_load $load_handle

        test "Test replica recovers connection failures" {
            # Wait until replica catches up
            wait_replica_online $master 0 1000 100
            wait_for_ofs_sync $master $replica

            # Verify db's are identical
            assert_morethan [$master dbsize] 0
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]
    set replica_pid  [srv 0 pid]

    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        test "Test master connection drops while streaming repl buffer into the db" {
            # Just after replica loads RDB, it will stream repl buffer into the
            # db. During streaming, we kill the master connection. Replica
            # will abort streaming and then try another psync with master.

            $master config set rdb-key-save-delay 1000
            $master config set repl-rdb-channel yes
            $master config set repl-diskless-sync yes
            $replica config set repl-rdb-channel yes
            $replica config set loading-process-events-interval-bytes 1024

            # Populate db and start write traffic
            populate 2000 master 1000
            set load_handle [start_write_load $master_host $master_port 100 "key1"]

            # Replica will pause in the loop of repl buffer streaming
            $replica debug repl-pause on-streaming-repl-buf
            $replica replicaof $master_host $master_port

            # Check if repl stream accumulation is started.
            wait_for_condition 50 1000 {
                [s -1 replica_repl_pending_data_size] > 0
            } else {
                fail "repl stream accumulation not started"
            }

            # Wait until replica starts streaming repl buffer
            wait_for_log_messages -1 {"*Starting to stream replication buffer*"} 0 2000 10
            stop_write_load $load_handle
            $master config set rdb-key-save-delay 0

            # Kill master connection and resume the process
            $replica deferred 1
            $replica client kill type master
            $replica debug repl-pause clear
            resume_process $replica_pid
            $replica read
            $replica read
            $replica deferred 0

            wait_for_log_messages -1 {"*Master client was freed while streaming*"} 0 500 10

            # Quick check for stats test coverage
            assert_morethan_equal [s -1 replica_repl_pending_data_peak] [s -1 replica_repl_pending_data_size]

            # Wait until replica recovers and verify db's are identical
            wait_replica_online $master 0 1000 10
            wait_for_ofs_sync $master $replica

            assert_morethan [$master dbsize] 0
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]
    set replica_pid  [srv 0 pid]

    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        test "Test main channel connection drops while loading rdb (disk based)" {
            # While loading rdb, we kill main channel connection.
            # We expect replica to complete loading RDB and then try psync
            # with the master.
            $master config set repl-rdb-channel yes
            $replica config set repl-rdb-channel yes
            $replica config set repl-diskless-load disabled
            $replica config set key-load-delay 10000
            $replica config set loading-process-events-interval-bytes 1024

            # Populate db and start write traffic
            populate 10000 master 100
            $replica replicaof $master_host $master_port

            # Wait until replica starts loading
            wait_for_condition 50 200 {
                [s -1 loading] == 1
            } else {
                fail "replica did not start loading"
            }

            # Kill replica connections
            $master client kill type replica
            $master set x 1

            # At this point, we expect replica to complete loading RDB. Then,
            # it will try psync with master.
            wait_for_log_messages -1 {"*Aborting rdb channel sync while loading the RDB*"} 0 2000 10
            wait_for_log_messages -1 {"*After loading RDB, replica will try psync with master*"} 0 2000 10

            # Speed up loading
            $replica config set key-load-delay 0

            # Wait until replica becomes online
            wait_replica_online $master 0 100 100

            # Verify there is another successful psync and no other full sync
            wait_for_condition 50 200 {
                [s 0 sync_full] == 1 &&
                [s 0 sync_partial_ok] == 2
            } else {
                fail "psync was not successful [s 0 sync_full] [s 0 sync_partial_ok]"
            }

            # Verify db's are identical after recovery
            wait_for_ofs_sync $master $replica
            assert_morethan [$master dbsize] 0
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl external:skip"}} {
    set replica [srv 0 client]
    set replica_pid  [srv 0 pid]

    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        test "Test main channel connection drops while loading rdb (diskless)" {
            # While loading rdb, kill both main and rdbchannel connections.
            # We expect replica to abort sync and later retry again.
            $master config set repl-rdb-channel yes
            $replica config set repl-rdb-channel yes
            $replica config set repl-diskless-load swapdb
            $replica config set key-load-delay 10000
            $replica config set loading-process-events-interval-bytes 1024

            # Populate db and start write traffic
            populate 10000 master 100

            $replica replicaof $master_host $master_port

            # Wait until replica starts loading
            wait_for_condition 50 200 {
                [s -1 loading] == 1
            } else {
                fail "replica did not start loading"
            }

            # Kill replica connections
            $master client kill type replica
            $master set x 1

            # At this point, we expect replica to abort loading RDB.
            wait_for_log_messages -1 {"*Aborting rdb channel sync while loading the RDB*"} 0 2000 10
            wait_for_log_messages -1 {"*Failed trying to load the MASTER synchronization DB from socket*"} 0 2000 10

            # Speed up loading
            $replica config set key-load-delay 0
            stop_write_load $load_handle

            # Wait until replica recovers and becomes online
            wait_replica_online $master 0 100 100

            # Verify replica attempts another full sync
            wait_for_condition 50 200 {
                [s 0 sync_full] == 2 &&
                [s 0 sync_partial_ok] == 2
            } else {
                fail "sync was not successful [s 0 sync_full] [s 0 sync_partial_ok]"
            }

            # Verify db's are identical after recovery
            wait_for_ofs_sync $master $replica
            assert_morethan [$master dbsize] 0
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}
