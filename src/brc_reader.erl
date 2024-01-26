-module(brc_reader).

-export([start/2]).

-define(TWO_MB, 1024*1024*2).

start(File, Pid) ->
  spawn_link(fun() ->
               read_loop(atom_to_list(File), Pid, 0, undefined, ?TWO_MB)
             end).

read_loop(File, Pid, 0, undefined, Size) ->
  {ok, FD} = prim_file:open(File, [read, binary, raw, read_ahead]),
  read_loop(File, Pid, 0, FD, Size);
read_loop(File, Pid, Offset, FD, Size) ->
  {NewOffset, OffsetSizes} = lists:foldl(fun(_, {O, Acc}) -> {O + Size, [{O, Size} | Acc]} end, {Offset, []}, lists:seq(1,5)),
  {ok, Data} = prim_file:pread(FD, lists:reverse(OffsetSizes)),
  case send_data(Data, Pid) of
    eof ->
      prim_file:close(FD),
      Pid ! done;
    ok ->
      read_loop(File, Pid, NewOffset, FD, Size)
  end.

send_data([], _Pid) -> ok;
send_data([eof | _], _Pid) -> eof;
send_data([Data | Rest], Pid) ->
  Pid ! {chunk, Data},
  send_data(Rest, Pid).