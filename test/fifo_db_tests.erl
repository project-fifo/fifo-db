-module(fifo_db_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_DIR, "test/data").

init() ->
    case proplists:is_defined(
           fifo_db,
           application:which_applications()) of
        true ->
            ok;
        false ->
            ?assertEqual(ok, application:start(erocksdb)),
            ?assertEqual(ok, application:start(eleveldb)),
            ?assertEqual(ok, application:start(syntax_tools)),
            ?assertEqual(ok, application:start(compiler)),
            ?assertEqual(ok, application:start(goldrush)),
            ?assertEqual(ok, application:start(lager)),
            ?assertEqual(ok, application:start(fifo_db)),
            application:set_env(fifo_db, db_path, ?TEST_DIR)
    end.

clean_data() ->
    os:cmd("rm -r " ?TEST_DIR "/*").

db_tester(Name, Backend) ->
    ?assertEqual(ok, init()),
    {ok, _} = fifo_db:start(Name, Backend, []),
    %% Create some keys and read them to check if they're OK.
    ?assertEqual(ok, fifo_db:put(Name, <<"B1">>, <<"K1">>, 11)),
    ?assertEqual({ok, 11}, fifo_db:get(Name, <<"B1">>, <<"K1">>)),
    ?assertEqual(ok, fifo_db:put(Name, <<"B1">>, <<"K2">>, 12)),
    ?assertEqual({ok, 12}, fifo_db:get(Name, <<"B1">>, <<"K2">>)),
    ?assertEqual(ok, fifo_db:put(Name, <<"B1">>, <<"K3">>, 12)),
    ?assertEqual({ok, 12}, fifo_db:get(Name, <<"B1">>, <<"K3">>)),
    %% Change the value of a key
    ?assertEqual(ok, fifo_db:put(Name, <<"B1">>, <<"K3">>, 13)),
    ?assertEqual({ok, 13}, fifo_db:get(Name, <<"B1">>, <<"K3">>)),
    %% Check if non existing keys are not found
    ?assertEqual(not_found, fifo_db:get(Name, <<"B1">>, <<"K4">>)),
    %% Check if we can lust the keys
    ?assertEqual([<<"K1">>, <<"K2">>, <<"K3">>],
                 lists:sort(fifo_db:list_keys(Name, <<"B1">>))),
    %% Delete a key and check if it is no longer found
    ?assertEqual(ok, fifo_db:delete(Name, <<"B1">>, <<"K3">>)),
    ?assertEqual(not_found, fifo_db:get(Name, <<"B1">>, <<"K3">>)),
    ?assertEqual([<<"K1">>, <<"K2">>],
                 lists:sort(fifo_db:list_keys(Name, <<"B1">>))),
    %% Check if list_keys be generted with fold_keys
    FoldFn = fun(K, Ks) ->
                     [K | Ks]
             end,
    ?assertEqual([<<"K1">>, <<"K2">>],
                 lists:sort(fifo_db:fold_keys(Name, <<"B1">>, FoldFn, []))),
    %% Fold ofer the keys and create a dict.
    Dict = fifo_db:fold(Name, <<"B1">>, fun orddict:store/3, []),
    ?assertEqual([{<<"K1">>, 11}, {<<"K2">>, 12}], Dict),
    clean_data(),
    ok.

erocksdb_test() ->
    db_tester(test_rocksdb, fifo_db_rocksdb).

eleveldb_test() ->
    db_tester(test_leveldb, fifo_db_leveldb).
