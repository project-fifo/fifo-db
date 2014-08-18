%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(fifo_db_leveldb).

-behaviour(fifo_db_driver).

%% API
-export([init/3, put/5, transact/3, get/4, fold/5, fold_keys/5,
         delete/4, terminate/2, code_change/3, list_keys/3]).

-ignore_xref([init/3, put/5, transact/3, get/4, fold/5, fold_keys/5,
              delete/4, terminate/2, code_change/3, list_keys/3]).

-record(state, {db}).

%%%===================================================================
%%% API
%%%===================================================================


open_opts([K | Ks], Opts) ->
    case proplists:is_defined(K, Opts) of
        true ->
            open_opts(Ks, Opts);
        _ ->
            case application:get_env(eleveldb, K) of
                {ok, V} ->
                    open_opts(Ks, [{K, V} | Opts]);
                _ ->
                    open_opts(Ks, Opts)
            end
    end;

open_opts([], Opts) ->
    Opts.

init(DBLoc, Name, Opts) ->
    Keys = [total_leveldb_mem_percent, total_leveldb_mem, limited_developer_mem,
            use_bloomfiltar, sst_block_size, block_restart_interval,
            verify_compaction, eleveldb_threads, fadvise_willneed,
            delete_threshold, mmap_size],
    Opts1 = open_opts(Keys, Opts),
    Opts2 = case proplists:is_defined(create_if_misisng, Opts1) of
                true ->
                    Opts1;
                false ->
                    [{create_if_missing, true} | Opts1]
            end,
    {ok, Db} = eleveldb:open(DBLoc ++ "/" ++ atom_to_list(Name), Opts2),
    {ok, #state{db = Db}}.

put(Bucket, Key, Value, _From, State) ->
    R = eleveldb:put(State#state.db, <<Bucket/binary, Key/binary>>,
                     term_to_binary(Value), []),
    {reply, R, State}.

transact(Transaction, _From, State) ->
    R = transact_int(State#state.db, Transaction),
    {reply, R, State}.

get(Bucket, Key, From, State) ->
    spawn(
      fun () ->
              case eleveldb:get(State#state.db,
                                <<Bucket/binary, Key/binary>>, []) of
                  {ok, Bin} ->
                      gen_server:reply(From, {ok, binary_to_term(Bin)});
                  E ->
                      gen_server:reply(From, E)
              end
      end),
    {noreply, State}.

delete(Bucket, Key, _From, State) ->
    R = eleveldb:delete(State#state.db, <<Bucket/binary, Key/binary>>, []),
    {reply, R, State}.

fold(Bucket, FoldFn, Acc0, From, State) ->
    spawn(
      fun () ->
              Len = byte_size(Bucket),
              try eleveldb:fold(State#state.db,
                                fun ({<<ThisBucket:Len/binary, Key/binary>>,
                                      Value}, Acc)
                                      when Bucket =:= ThisBucket ->
                                        FoldFn(Key, binary_to_term(Value), Acc);
                                    ({_, _}, Acc) ->
                                        throw({ok, Acc})
                                end, Acc0, [{first_key, Bucket}]) of
                  R ->
                      gen_server:reply(From, R)
              catch
                  R ->
                      gen_server:reply(From, R)
              end
      end),
    {noreply, State}.

fold_keys(Bucket, FoldFn, Acc0, From, State) ->
    spawn(
      fun () ->
              Len = byte_size(Bucket),
              try eleveldb:fold_keys(State#state.db,
                                     fun (<<ThisBucket:Len/binary, Key/binary>>,
                                          Acc)
                                           when Bucket =:= ThisBucket ->
                                             FoldFn(Key, Acc);
                                         (_, Acc) ->
                                             throw({ok, Acc})
                                     end, Acc0, [{first_key, Bucket}]) of
                  R ->
                      gen_server:reply(From, R)
              catch
                  R ->
                      gen_server:reply(From, R)
              end

      end),
    {noreply, State}.

list_keys(Bucket, _From, State) ->
    FoldFn = fun(K, Ks) ->
                     [K | Ks]
             end,
    fold_keys(Bucket, FoldFn, [], _From, State).


terminate(_Reason, #state{db = Db}) ->
    eleveldb:close(Db).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

transact_int(DB, [{put, K, V} | R]) ->
    eleveldb:put(DB, K, V, []),
    transact_int(DB, R);
transact_int(DB, [{delete, K} | R]) ->
    eleveldb:delete(DB, K, []),
    transact_int(DB, R);
transact_int(_DB, []) ->
    ok.
