-module(fifo_db_driver).

-export([encode_key/1, encode_key/2, decode_key/1]).

-type calback_reply() :: calback_reply(any()).
-type calback_reply(ReplyType) :: {noreply, State::term()} |
                         {reply, Reply::ReplyType, State::term()}.

-type fold_fn() :: fun((Key::binary(), Value::term(), Acc) -> Acc).

-type fold_key_fn() :: fun((Key::binary(), Acc) -> Acc).

-type transaction_op() :: {delete, Key::binary()} |
                          {put, Key::binary(), Value::term()}.



-callback init(DBLock::string(), Name::atom(), Opts::[term()]) ->
    {ok, State::term()}.

-callback put(Bucket::binary(), Keyt::binary(), Value::term(), _From::pid(),
              State::term()) ->
    calback_reply().
-callback transact(Transaction::[transaction_op()], From::pid(),
                   State::term()) ->
    calback_reply().
-callback get(Buckett::binary(), Key::binary(), From::pid(), State::term()) ->
    calback_reply().
-callback delete(Bucket::binary(), Key::binary(), _From::pid(),
                 State::term()) ->
    calback_reply().
-callback fold(Bucket::binary(), FoldFn::fold_fn(), Acc0::term(),
               From::pid(), State::term()) ->
    calback_reply().
-callback fold_keys(Bucket::binary(), FoldFn::fold_key_fn(), Acc0::term(),
                    From::pid(), State::term()) ->
    calback_reply().
-callback list_keys(Bucket::binary(), _From::pid(), State::term()) ->
    calback_reply([binary()]).

-callback terminate(Reason::atom(), State::term()) ->
    ok.
-callback code_change(OldVsn::term(), State::term(), Extra::term()) ->
    {ok, State::term()}.


-spec encode_key({Bucket::binary(), Key::binary()}) -> binary().
encode_key({Bucket, Key}) ->
    encode_key(Bucket, Key).

-spec encode_key(Bucket::binary(), Key::binary()) -> binary().
encode_key(Bucket, Key) ->
    Len = byte_size(Bucket),
    <<Len:16, Bucket/binary, Key/binary>>.

-spec decode_key(BucketKey::binary()) -> {Bucket::binary(), Key::binary()}.
decode_key(<<Len:16, Bucket:Len/binary, Key/binary>>) ->
    {Bucket, Key}.

