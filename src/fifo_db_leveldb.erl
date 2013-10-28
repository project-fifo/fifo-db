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
-export([init/3, put/4, transact/2, get/3, fold/4, fold_keys/4,
         delete/3, terminate/2, code_change/3, list_keys/2]).

-ignore_xref([init/3, put/4, transact/2, get/3, fold/4, fold_keys/4,
              delete/3, terminate/2, code_change/3, list_keys/2]).

-record(state, {db}).

%%%===================================================================
%%% API
%%%===================================================================

init(DBLoc, Name, _) ->
    {ok, Db} = eleveldb:open(DBLoc ++ "/" ++ atom_to_list(Name),
                             [{create_if_missing, true}]),
    {ok, #state{db = Db}}.

put(Bucket, Key, Value, State) ->
    R = eleveldb:put(State#state.db, <<Bucket/binary, Key/binary>>,
                     term_to_binary(Value), []),
    {R, State}.

transact(Transaction, State) ->
    R = transact_int(State#state.db, Transaction),
    {R, State}.

get(Bucket, Key, State) ->
    case eleveldb:get(State#state.db, <<Bucket/binary, Key/binary>>, []) of
        {ok, Bin} ->
            {{ok, binary_to_term(Bin)}, State};
        E ->
            {E, State}
    end.

delete(Bucket, Key, State) ->
    R = eleveldb:delete(State#state.db, <<Bucket/binary, Key/binary>>, []),
    {R, State}.

fold(Bucket, FoldFn, Acc0, State) ->
    Len = byte_size(Bucket),
    R = eleveldb:fold(State#state.db,
                      fun ({<<ThisBucket:Len/binary, Key/binary>>, Value}, Acc)
                            when Bucket =:= ThisBucket ->
                              FoldFn(Key, binary_to_term(Value), Acc);
                          ({_, _}, Acc) ->
                              Acc
                      end, Acc0, []),
    {R, State}.

fold_keys(Bucket, FoldFn, Acc0, State) ->
    Len = byte_size(Bucket),
    R = eleveldb:fold_keys(State#state.db,
                           fun (<<ThisBucket:Len/binary, Key/binary>>, Acc)
                                 when Bucket =:= ThisBucket ->
                                   FoldFn(Key, Acc);
                               (_, Acc) ->
                                   Acc
                           end, Acc0, []),
    {R, State}.

list_keys(Bucket, State) ->
    FoldFn = fun(K, Ks) ->
                     [K | Ks]
             end,
    fold_keys(Bucket, FoldFn, [], State).


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
