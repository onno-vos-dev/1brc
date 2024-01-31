-ifndef(_HASH_HRL_).
-define(_HASH_HRL_, true).

%% This is a modified version of the FNV64a hashing code found at:
%% https://github.com/leostera/erlang-hash/blob/a1b9101189e115b4eabbe941639f3c626614e986/src/hash_fnv.erl#L98
%%
%% The reason is that PropEr kept finding conflicting keys such as for example: <<"JiÔk">> & <<"næðl">>
%% which both produce the same FNV64a Hash.
%%
%% So we'll call this the 1BRC hash :-)
-define(PRIME, 16777619).
-define(INIT,  2166136261).
-define(MASK,  16#FFFFFFFF).
-define(HASH(Acc, Char), ((Char * Char + Char) + (Acc bxor Char) * ?PRIME) band ?MASK).

-endif.