set testmodule [file normalize tests/modules/rdbloadsave.so]

start_server {tags {"modules"}} {
    proc format_command {args} {
        set cmd "*[llength $args]\r\n"
        foreach a $args {
            append cmd "$[string length $a]\r\n$a\r\n"
        }
        set _ $cmd
    }

    r module load $testmodule

    test "Module rdbloadsave sanity" {
        r test.sanity

        assert_error {*No such file*} {r test.rdbload sanity.rdb}

        r set x 1
        assert_equal OK [r test.rdbsave sanity.rdb]

        r flushdb
        assert_equal OK [r test.rdbload sanity.rdb]
        assert_equal 1 [r get x]
    }

    test "Module rdbloadsave test with pipelining" {
        r config set save ""
        r config set loading-process-events-interval-bytes 1024
        r flushdb

        populate 50000 a 128
        r set x 111
        assert_equal [r dbsize] 50001

        assert_equal OK [r test.rdbsave blabla.rdb]
        r flushdb
        assert_equal [r dbsize] 0

        # Send commands with pipeline. First command will call RM_RdbLoad() in
        # the command callback. While loading RDB, Redis can go to networking to
        # reply -LOADING. By sending commands in pipeline, we verify it doesn't
        # cause a problem.
        # e.g. Redis won't try to process next message of the current client
        # while it is in the command callback.
        r write [format_command test.rdbload blabla.rdb]
        r flush
        r write [format_command get x]
        r write [format_command dbsize]
        r flush

        assert_equal OK [r read]
        assert_equal 111 [r read]
        assert_equal 50001 [r read]
    }

    test "Module rdbloadsave with aof" {
        r config set save ""

        # Enable the AOF
        r config set appendonly yes
        r config set auto-aof-rewrite-percentage 0 ; # Disable auto-rewrite.
        waitForBgrewriteaof r

        r set k v1
        assert_equal OK [r test.rdbsave aoftest.rdb]

        r set k v2
        r config set rdb-key-save-delay 10000000
        r bgrewriteaof

        # RM_RdbLoad() should kill aof fork
        assert_equal OK [r test.rdbload aoftest.rdb]

        wait_for_condition 50 100 {
            [string match {*Killing*AOF*child*} [exec tail -20 < [srv 0 stdout]]]
        } else {
            fail "Can't find 'Killing AOF child' in recent log lines"
        }

        # Verify the value in the loaded rdb
        assert_equal v1 [r get k]

        r flushdb
        r config set rdb-key-save-delay 0
        r config set appendonly no
    }

    test "Module rdbloadsave with bgsave" {
        r flushdb
        r config set save ""

        r set k v1
        assert_equal OK [r test.rdbsave bgsave.rdb]

        r set k v2
        r config set rdb-key-save-delay 10000000
        r bgsave

        # RM_RdbLoad() should kill RDB fork
        assert_equal OK [r test.rdbload bgsave.rdb]

        wait_for_condition 10 1000 {
            [string match {*SIGUSR1*child*} [exec tail -20 < [srv 0 stdout]]]
        } else {
            fail "Can't find 'SIGUSR1 child' in recent log lines"
        }

        assert_equal v1 [r get k]
        r flushall
        waitForBgsave r

        # RM_RdbSave() should fail if there is already a fork saving into the
        # same rdb file.
        r set k v3
        r bgsave
        assert_error {*in*progress*} {r test.rdbsave dump.rdb}

        r flushall
        waitForBgsave r
        r config set rdb-key-save-delay 0
    }

    test "Module rdbloadsave calls rdbsave in a module fork" {
        r flushdb
        r config set save ""
        r config set rdb-key-save-delay 3000000

        r set k v1

        # Module will call RM_Fork() before calling RM_RdbSave()
        assert_equal OK [r test.rdbsave_fork rdbfork.rdb]
        assert_equal [s module_fork_in_progress] 1

        wait_for_condition 10 1000 {
            [status r module_fork_in_progress] == "0"
        } else {
            fail "Module fork didn't finish"
        }

        r set k v2
        assert_equal OK [r test.rdbload rdbfork.rdb]
        assert_equal v1 [r get k]

        r config set rdb-key-save-delay 0
    }

    test "Unload the module - rdbloadsave" {
        assert_equal {OK} [r module unload rdbloadsave]
    }
}
