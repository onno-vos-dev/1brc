-ifndef(_HASH_HRL_).
-define(_HASH_HRL_, true).

-define(FNV32_PRIME, 16777619).
-define(FNV32_INIT,  2166136261).
-define(FNV32_MASK,  16#FFFFFFFF).
-define(FNV32_HASH(Acc, Char), ((Acc bxor Char) * ?FNV32_PRIME) band ?FNV32_MASK).

-endif.