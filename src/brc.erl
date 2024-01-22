-module(brc).

-export([run/1]).

run([File]) ->
  Workers = brc_workers:spawn_workers(erlang:system_info(logical_processors)),
  {Pid, Ref} = erlang:spawn_monitor(fun() -> exit({normal, brc_processor:start(Workers)}) end),
  brc_reader:start(File, Pid),
  receive
    {'DOWN', Ref, process, Pid, {normal, Result}} ->
      [ exit(P, kill) || P <- Workers ],
      format_output(Result);
      %% ok;
    {'DOWN', Ref, process, Pid, Reason} ->
      error({worker_died, Reason})
  end.

format_output(Map) ->
  Calculated = maps:fold(fun(K, {Min, Max, MeasurementsAcc, N}, A) -> [{K, {round_back(Min), round_back(Max), round_back(MeasurementsAcc / N)}} | A] end, [], Map), lists:sort(Calculated).

round_back(IntFloat) ->
  list_to_float(io_lib:format("~.1f", [IntFloat / 10])).