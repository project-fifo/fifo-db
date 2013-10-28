%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(fifo_db_hanoidb).

-behaviour(fifo_db_driver).

%% API
-export([init/3, put/5, transact/3, get/4, fold/5, fold_keys/5,
         delete/4, terminate/2, code_change/3, list_keys/3]).

-ignore_xref([init/3, put/5, transact/3, get/4, fold/5, fold_keys/5,
              delete/4, terminate/2, code_change/3, list_keys/3]).

-record(state, {db}).

-include("hanoidb.hrl").

%%%===================================================================
%%% API
%%%===================================================================

init(DBLoc, Name, _) ->
    {ok, Db} = hanoidb:open(DBLoc ++ "/" ++ atom_to_list(Name)),
    {ok, #state{db = Db}}.

put(Bucket, Key, Value, _From, State) ->
    R = hanoidb:put(State#state.db, <<Bucket/binary, Key/binary>>,
                    term_to_binary(Value)),
    {reply, R, State}.

transact(Transaction, _From, State) ->
    R = hanoidb:transact(State#state.db, Transaction),
    {reply, R, State}.

get(Bucket, Key, From, State) ->
    spawn(
      fun () ->
              case hanoidb:get(State#state.db, <<Bucket/binary, Key/binary>>) of
                  {ok, Bin} ->
                      gen_server:reply(From, {ok, binary_to_term(Bin)});
                  E ->
                      gen_server:reply(From, E)
              end
      end),
    {noreply, State}.

delete(Bucket, Key, _From, State) ->
    R = hanoidb:delete(State#state.db, <<Bucket/binary, Key/binary>>),
    {reply, R, State}.

fold(Bucket, FoldFn, Acc0, From, State) ->
    spawn(
      fun () ->
              R = int_fold(State#state.db, Bucket,
                           fun(Key, Value, Acc) ->
                                   FoldFn(Key, binary_to_term(Value), Acc)
                           end, Acc0),
              gen_server:reply(From, R)
      end),
    {noreply, State}.

fold_keys(Bucket, FoldFn, Acc0, From, State) ->
    spawn(
      fun () ->
              R = int_fold(State#state.db, Bucket,
                           fun(Key, _, Acc) ->
                                   FoldFn(Key, Acc)
                           end, Acc0),
              gen_server:reply(From, R)
      end),
    {noreply, State}.

list_keys(Bucket, _From, State) ->
    FoldFn = fun(K, Ks) ->
                     [K | Ks]
             end,
    fold_keys(Bucket, FoldFn, [], _From, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{db = Db}) ->
    hanoidb:close(Db).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
int_fold(Db, Bucket, FoldFn, Acc0) ->
    Len = byte_size(Bucket),
    L = Len - 1,
    <<Prefix:L/binary, R>> = Bucket,
    R1 = R + 1,
    End = <<Prefix/binary, R1>>,
    Range = #key_range{
      from_key = Bucket,
      from_inclusive = false,
      to_key = End,
      to_inclusive = false
     },
    hanoidb:fold_range(Db,
                       fun (<<_:Len/binary, Key/binary>>, Value, Acc) ->
                               FoldFn(Key, Value, Acc);
                           (Key, _, Acc) ->
                               lager:error("[db/~p] Unknown fold key '~p'.", [Bucket, Key]),
                               Acc
                       end,
                       Acc0,
                       Range).
