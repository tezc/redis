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

    test "Module bgsave" {
        r config set save ""
        # 5000 keys with 1ms sleep per key should take 5 second
        r config set rdb-key-save-delay 10
        # r config set loading-process-events-interval-bytes 1024
        populate 200000 a
        r set x 111
        #r bgsave
        #assert_equal [s rdb_bgsave_in_progress] 1
        assert_equal [r dbsize] 200001

        assert_equal OK [r test.blocked_client_rdbsave 0 blabla.rdb]
        r flushdb
        assert_equal [r dbsize] 0

        r write [format_command test.blocked_client_rdbload 0 blabla.rdb]
        r flush
        r write [format_command get x]
        r flush
        assert_equal OK [r read]
        # assert_equal 111 [r read]
        # assert_equal OK [r test.blocked_client_rdbload blabla.rdb]
        # assert_equal [r dbsize] 2000001
        # assert_equal [r get x] 111
    }

    test "Module sanity" {
        r test.sanity
    }

    test "Module load unload" {
        # assert_error {*rdbload*} {r test.blocked_client_rdbload unload.rdb}
        # assert_equal OK [r test.blocked_client_rdbsave unload.rdb]
        # assert_equal OK [r test.blocked_client_rdbload unload.rdb]
    }

    test "Module large state save" {

    }



    test "Unload the module - eventloop" {
        assert_equal {OK} [r module unload rdbloadsave]
    }
}
