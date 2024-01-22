-module(brc_workers).

-export([spawn_workers/1]).

spawn_workers(N) ->
  spawn_workers(N, []).

spawn_workers(0, Acc) -> Acc;
spawn_workers(N, Acc) ->
  Pid = erlang:spawn(fun() -> worker() end),
  spawn_workers(N - 1, [Pid | Acc]).

worker() ->
  process_flag(message_queue_data, off_heap),
  receive
    {chunk, Chunk} ->
      handle_complete_chunk(Chunk),
      worker();
    {Parent, no_more_work} ->
      Parent ! {self(), worker_done, maps:from_list(get())},
      exit(self(), kill)
    end.

handle_complete_chunk(Chunk) ->
  Chunks = binary:split(Chunk, [<<";">>, <<"\n">>], [global]),
  do_handle_complete_chunk(Chunks).

do_handle_complete_chunk([]) ->
  ok;
do_handle_complete_chunk([<<>> | Others]) ->
  do_handle_complete_chunk(Others);
do_handle_complete_chunk([City, Measurement | Others]) ->
  MeasurementFloat = parse_float(Measurement),
  case get(City) of
    undefined ->
      put(City, {MeasurementFloat, MeasurementFloat, MeasurementFloat, 1});
    {Min, Max, MeasurementAcc, N} ->
      put(City, {min(Min, MeasurementFloat), max(Max, MeasurementFloat), MeasurementAcc + MeasurementFloat, N + 1})
  end,
  do_handle_complete_chunk(Others).

%% Very specialized float-parser for floats with a single fractional
%% digit, and returns the result as an integer * 10.
%% Credits to @Jesperes: https://github.com/jesperes/erlang_1brc/commit/7dc7eff0f158c8a666c24033f969f47a9f2a330e
-define(TO_NUM(C), (C - $0)).

parse_float(<<$-, A, B, $., C>>) ->
  -1 * (?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C));
parse_float(<<$-, B, $., C>>) ->
  -1 * (?TO_NUM(B) * 10 + ?TO_NUM(C));
parse_float(<<A, B, $., C>>) ->
  ?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C);
parse_float(<<B, $., C>>) ->
  ?TO_NUM(B) * 10 + ?TO_NUM(C).