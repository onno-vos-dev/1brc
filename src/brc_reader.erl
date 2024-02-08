-module(brc_reader).

-export([start/2]).

-define(ONE_MB, 1024*1024*10).

start(File0, Pid) ->
  spawn_link(fun() ->
                File = atom_to_list(File0),
                FileSize = filelib:file_size(File),
                read_loop(File, Pid, 0, undefined, {0, ?ONE_MB, FileSize})
             end).

read_loop(File, Pid, 0, undefined, {N, ChunkSize, FileSize}) ->
  Start = erlang:monotonic_time(),
  put(start, Start),
  {ok, FD} = prim_file:open(File, [read, binary, raw, read_ahead]),
  read_loop(File, Pid, 0, FD, {N, ChunkSize, FileSize});
read_loop(File, Pid, Offset, FD, {N, ChunkSize, FileSize}) when N =< 10 ->
  case read_and_send(FD, Offset, ChunkSize, Pid) of
    eof ->
      ok;
    continue ->
      read_loop(File, Pid, Offset + ChunkSize, FD, {N + 1, ChunkSize, FileSize})
  end;
read_loop(File, Pid, Offset, FD, {N, _ChunkSize, FileSize}) when N > 10 ->
  NewChunkSize = FileSize div 100,
  case read_and_send(FD, Offset, NewChunkSize, Pid) of
    eof ->
      ok;
    continue ->
      read_loop(File, Pid, Offset + NewChunkSize, FD, {N + 1, NewChunkSize, FileSize})
  end.

read_and_send(FD, Offset, Size, Pid) ->
  case prim_file:pread(FD, Offset, Size) of
    eof ->
      prim_file:close(FD),
      Now = erlang:monotonic_time(),
      Start = get(start),
      io:format("Sending all data took: ~p~n", [(Now - Start) / 1000_000_000.0]),
      Pid ! done,
      eof;
    {ok, Data} ->
      Pid ! {chunk, Data},
      continue
  end.