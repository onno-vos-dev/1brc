-module(brc_reader).

-export([start/1]).

-define(ONE_MB, 1048576).
-define(TWO_MB, ?ONE_MB * 2).

start(Pid) ->
  spawn_link(fun() ->
               read_loop(Pid, 0, undefined, ?TWO_MB)
             end).

read_loop(Pid, 0, undefined, Size) ->
  {ok, FD} = prim_file:open("measurements.txt", [read, binary, raw, read_ahead]),
  read_loop(Pid, 0, FD, Size);
read_loop(Pid, Offset, FD, Size) ->
  {NewOffset, OffsetSizes} = lists:foldl(fun(_, {O, Acc}) -> {O + Size, [{O, Size} | Acc]} end, {Offset, []}, lists:seq(1,5)),
  {ok, Data} = prim_file:pread(FD, lists:reverse(OffsetSizes)),
  case send_data(Data, Pid) of
    eof ->
      prim_file:close(FD),
      Pid ! done;
    ok ->
      read_loop(Pid, NewOffset, FD, Size)
  end.

send_data([], _Pid) -> ok;
send_data([eof | _], _Pid) -> eof;
send_data([Data | Rest], Pid) ->
  Pid ! {chunk, Data},
  send_data(Rest, Pid).