-ifndef(_HASH_HRL_).
-define(_HASH_HRL_, true).


%-define(HASH(Acc, Char), (((Acc band (Char + ?MASK)) + (Char + ?PRIME)) + (((Acc + Char) bxor Char) * (?PRIME band Char) band ?MASK))).

%% This is a modified version of the FNV64a hashing code found at:
%% https://github.com/leostera/erlang-hash/blob/a1b9101189e115b4eabbe941639f3c626614e986/src/hash_fnv.erl#L98
%%
%% The reason is that PropEr kept finding conflicting keys
%% which both produce the same FNV Hash. Hence, we test that all cities from the test file containing 128.660 cities
%% are hasheable with below hash. See brc.erl eunit test for that.
%%
%% So we'll call this just hash :-)
-define(PRIME, 16777619).
-define(INIT,  2166136261).
-define(MASK,  16#FFFFFFFF).
-define(HASH(Acc, Char), (((Acc bxor Char) * ?PRIME + 1)) band ?MASK).

-endif.