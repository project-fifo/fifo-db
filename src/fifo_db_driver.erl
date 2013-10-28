-module(fifo_db_driver).

-export([behaviour_info/1]).

-export([encode_key/1, encode_key/2, decode_key/1]).

behaviour_info(callbacks) ->
    [{init, 3},
     {put, 5},
     {transact, 3},
     {get, 4},
     {fold, 5},
     {fold_keys, 5},
     {list_keys, 3},
     {delete, 4},
     {terminate, 2},
     {code_change, 3}];

behaviour_info(_Other) ->
    undefined.

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

