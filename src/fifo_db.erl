%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2013, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2013 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------

-module(fifo_db).

-behaviour(gen_server).

%% API
-export([start_link/3,
         start/1,
         start/2,
         start/3,
         sync_get/3,
         get/3,
         transact/2,
         delete/3,
         put/4,
         fold/4,
         fold_keys/4,
         list_keys/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ignore_xref([start_link/1, fold_keys/4]).


%%%===================================================================
%%% API
%%%===================================================================
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start_link(Name, Backend, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Backend, Opts], []).

start(Name) ->
    start(Name, []).

start(Name, Opts) ->
    start(Name, application:get_env(fifo_db, backend, fifo_db_leveldb), Opts).

start(Name, Backend, Opts) ->
    case erlang:whereis(Name) of
        undefined ->
            fifo_db_sup:start_child(Name, Backend, Opts);
        _ ->
            ok
    end.

transact(Name, Transaction) ->
    gen_server:call(Name, {transact, Transaction}).

put(Name, Bucket, Key, Value) ->
    gen_server:call(Name, {put, Bucket, Key, Value}).

get(Name, Bucket, Key) ->
    gen_server:call(Name, {get, Bucket, Key}).

sync_get(Name, Bucket, Key) ->
    gen_server:call(Name, {sync_get, Bucket, Key}).

delete(Name, Bucket, Key) ->
    gen_server:call(Name, {delete, Bucket, Key}).

fold(Name, Bucket, FoldFn, Acc0) ->
    gen_server:call(Name, {fold, Bucket, FoldFn, Acc0}).

fold_keys(Name, Bucket, FoldFn, Acc0) ->
    gen_server:call(Name, {fold_keys, Bucket, FoldFn, Acc0}).

list_keys(Name, Bucket) ->
    gen_server:call(Name, {list_keys, Bucket}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

init([Name, Backend, Opts]) when is_atom(Name),
                                 is_atom(Backend) ->
    {ok, DBLoc} = application:get_env(fifo_db, db_path),
    lager:info("Opening ~s  in ~s as backend ~p with options: ~p", [Name, DBLoc, Backend, Opts]),
    {ok, State} = Backend:init(DBLoc, Name, Opts),
    {ok, {Backend, State}};
init(P) ->
    lager:error("Invalid init parameters: ~p", [P]),
    {stop, error, undefined}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({put, Bucket, Key, Value}, From, {Backend, State}) ->
    case Backend:put(Bucket, Key, Value, From, State)  of
        {reply, Reply, State1} ->
            {reply, Reply, {Backend, State1}};
        {noreply, State1} ->
            {noreply, {Backend, State1}}
    end;

handle_call({transact, Transaction}, From, {Backend, State}) ->
    case Backend:transact(Transaction, From, State) of
        {reply, Reply, State1} ->
            {reply, Reply, {Backend, State1}};
        {noreply, State1} ->
            {noreply, {Backend, State1}}
    end;

handle_call({get, Bucket, Key}, From, {Backend, State}) ->
    case Backend:get(Bucket, Key, From, State) of
        {reply, Reply, State1} ->
            {reply, Reply, {Backend, State1}};
        {noreply, State1} ->
            {noreply, {Backend, State1}}
    end;

handle_call({delete, Bucket, Key}, From, {Backend, State}) ->
    case Backend:delete(Bucket, Key, From, State)  of
        {reply, Reply, State1} ->
            {reply, Reply, {Backend, State1}};
        {noreply, State1} ->
            {noreply, {Backend, State1}}
    end;

handle_call({list_keys, Bucket}, From, {Backend, State}) ->
    case Backend:list_keys(Bucket, From, State) of
        {reply, Reply, State1} ->
            {reply, Reply, {Backend, State1}};
        {noreply, State1} ->
            {noreply, {Backend, State1}}
    end;

handle_call({fold, Bucket, FoldFn, Acc0}, From, {Backend, State}) ->
    case Backend:fold(Bucket, FoldFn, Acc0, From, State) of
        {reply, Reply, State1} ->
            {reply, Reply, {Backend, State1}};
        {noreply, State1} ->
            {noreply, {Backend, State1}}
    end;

handle_call({fold_keys, Bucket, FoldFn, Acc0}, From, {Backend, State}) ->
    case Backend:fold_keys(Bucket, FoldFn, Acc0, From, State) of
        {reply, Reply, State1} ->
            {reply, Reply, {Backend, State1}};
        {noreply, State1} ->
            {noreply, {Backend, State1}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, From, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, {Backend, State}) ->
    Backend:terminate(Reason, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(OldVsn, {Backend, State}, Extra) ->
    {ok, State1} = Backend:code_change(OldVsn, State, Extra),
    {ok, {Backend, State1}}.
