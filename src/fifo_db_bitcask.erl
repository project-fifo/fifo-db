%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(fifo_db_bitcask).

-behaviour(fifo_db_driver).

%% API
-export([init/3, put/4, transact/2, get/3, fold/4, fold_keys/4,
         delete/3, terminate/2, code_change/3]).

-ignore_xref([init/3, put/4, transact/2, get/3, fold/4, fold_keys/4,
              delete/3, terminate/2, code_change/3]).

-include("bitcask.hrl").

-record(state, {db}).

%%%===================================================================
%%% API
%%%===================================================================

init(DBLoc, Name, _) ->
    Db = bitcask:open(DBLoc ++ "/" ++ atom_to_list(Name), [read_write]),
    {ok, #state{db = Db}}.

put(Bucket, Key, Value, State) ->
    R = bitcask:put(State#state.db, <<Bucket/binary, Key/binary>>, term_to_binary(Value)),
    {R, State}.


transact(Transaction, State) ->
    R = transact_int(State#state.db, Transaction),
    {R, State}.

get(Bucket, Key, State) ->
    case bitcask:get(State#state.db, <<Bucket/binary, Key/binary>>) of
        {ok, Bin} ->
            {{ok, binary_to_term(Bin)}, State};
        E ->
            {E, State}
    end.

delete(Bucket, Key, State) ->
    R = bitcask:delete(State#state.db, <<Bucket/binary, Key/binary>>),
    {R, State}.

fold(Bucket, FoldFn, Acc0, State) ->
    Len = byte_size(Bucket),
    R = bitcask:fold(State#state.db,
                     fun (<<ThisBucket:Len/binary, Key/binary>>, Value, Acc)
                           when Bucket =:= ThisBucket ->
                             FoldFn(Key, binary_to_term(Value), Acc);
                         (_, _, Acc) ->
                             Acc
                     end,
                     Acc0),
    {R, State}.

fold_keys(Bucket, FoldFn, Acc0, State) ->
    Len = byte_size(Bucket),
    R = bitcask:fold_keys(State#state.db,
                          fun (#bitcask_entry{
                                  key = <<ThisBucket:Len/binary, Key/binary>>
                                 }, Acc)
                                when Bucket =:= ThisBucket ->
                                  FoldFn(Key, Acc);
                              (_, Acc) ->
                                  Acc
                          end,
                          Acc0),
    {R, State}.

terminate(_Reason, #state{db = Db} = _State) ->
    bitcask:close(Db).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

transact_int(DB, [{put, K, V} | R]) ->
    bitcask:put(DB, K, V),
    transact_int(DB, R);
transact_int(DB, [{delete, K} | R]) ->
    bitcask:delete(DB, K),
    transact_int(DB, R);
transact_int(_DB, []) ->
    ok.
