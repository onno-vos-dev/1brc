-module(brc_workers).

-export([spawn_workers/1]).

-include("hash.hrl").

spawn_workers(N) ->
  spawn_workers(N, []).

spawn_workers(0, Acc) -> Acc;
spawn_workers(N, Acc) ->
  Pid = erlang:spawn_link(fun() ->
                            worker()
                          end),
  spawn_workers(N - 1, [Pid | Acc]).

worker() ->
  process_flag(message_queue_data, off_heap),
  receive
    {chunk, Chunk} ->
      process_lines(Chunk, ?FNV32_INIT),
      worker();
    {Parent, no_more_work} ->
      Parent ! {self(), worker_done, maps:from_list(get())}
    end.

process_lines(<<>>, _) -> ok;
process_lines(<<$;:8, Rest/binary>>, City) ->
  do_process_line(Rest, City);
process_lines(<<C1:8, $;:8, Rest/binary>>, Acc) ->
  do_process_line(Rest, ?FNV32_HASH(Acc, C1));
process_lines(<<C1:8, C2:8, $;:8, Rest/binary>>, Acc) ->
  do_process_line(Rest, ?FNV32_HASH(?FNV32_HASH(Acc, C1), C2));
process_lines(<<C1:8, C2:8, C3:8, $;:8, Rest/binary>>, Acc) ->
  do_process_line(Rest, ?FNV32_HASH(?FNV32_HASH(?FNV32_HASH(Acc, C1), C2), C3));
process_lines(<<C1:8, C2:8, C3:8, C4:8, Rest/binary>>, Acc) ->
  process_lines(Rest, ?FNV32_HASH(?FNV32_HASH(?FNV32_HASH(?FNV32_HASH(Acc, C1), C2), C3), C4)).

%% Very specialized float-parser for floats with a single fractional
%% digit, and returns the result as an integer * 10.
-define(TO_NUM(C), (C - $0)).
do_process_line(<<$-, A:8, B:8, $., C:8, $\n:8, Rest/binary>>, City) ->
  add_to_state(Rest, City, -1 * (?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C)));
do_process_line(<<$-, B:8, $., C:8, $\n:8, Rest/binary>>, City) ->
  add_to_state(Rest, City, -1 * (?TO_NUM(B) * 10 + ?TO_NUM(C)));
do_process_line(<<A:8, B:8, $., C:8, $\n:8, Rest/binary>>, City) ->
  add_to_state(Rest, City, ?TO_NUM(A) * 100 + ?TO_NUM(B) * 10 + ?TO_NUM(C));
do_process_line(<<B:8, $., C:8, $\n:8, Rest/binary>>, City) ->
  add_to_state(Rest, City, ?TO_NUM(B) * 10 + ?TO_NUM(C)).

add_to_state(<<Rest/binary>>, City, Measurement) ->
  case get(City) of
    undefined ->
      put(City, {Measurement, Measurement, Measurement, 1}),
      process_lines(Rest, ?FNV32_INIT);
    {Min, Max, MeasurementAcc, N} ->
      put(City, {min(Min, Measurement), max(Max, Measurement), MeasurementAcc + Measurement, N + 1}),
      process_lines(Rest, ?FNV32_INIT)
  end.