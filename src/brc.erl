-module(brc).

-export([run/1, find_cities/1]).

-include("hash.hrl").

run([File]) ->
  LkupTable = find_cities(atom_to_list(File)),
  Workers = brc_workers:spawn_workers(erlang:system_info(logical_processors)),
  {Pid, Ref} = erlang:spawn_monitor(fun() -> exit({normal, brc_processor:start(Workers)}) end),
  brc_reader:start(File, Pid),
  receive
    {'DOWN', Ref, process, Pid, {normal, Result}} ->
      [ exit(P, kill) || P <- Workers ],
      io:format("~ts~n", [format_output(Result, LkupTable)]);
    {'DOWN', Ref, process, Pid, Reason} ->
      error({worker_died, Reason})
  end.

format_output(Map, LkupTable) ->
  Fmts = lists:map(fun({K, {Min, Max, MeasurementsAcc, N}}) -> io_lib:format("~ts=~p/~p/~p", [maps:get(K, LkupTable), round_back(Min), round_back(Max), round_back(MeasurementsAcc / N)]) end, maps:to_list(Map)),
  "{" ++ lists:join(", ", Fmts) ++ "}".

round_back(IntFloat) ->
  list_to_float(io_lib:format("~.1f", [IntFloat / 10])).

find_cities(File) ->
  {ok, FD} = prim_file:open(File, [read, binary, raw, read_ahead]),
  {ok, Data} = prim_file:read(FD, 1024*1024*2), %% Read two megabytes only
  CityMeasurements = binary:split(Data, <<"\n">>, [global]),
  Cities = lists:usort(match_cities(CityMeasurements, [])),
  LkupTable = lists:foldl(fun(City, A) -> A#{storage_key(City) => City} end, #{}, Cities),
  true = length(Cities) =:= maps:size(LkupTable), %% assert
  LkupTable.

match_cities([], Acc) -> Acc;
match_cities([CityMeasurement | Others], Acc) ->
  case binary:match(CityMeasurement, <<";">>) of
    nomatch -> Acc;
    {Pos, _Len} ->
      match_cities(Others, [binary:part(CityMeasurement, 0, Pos) | Acc])
  end.

storage_key(City) ->
  lists:foldl(fun(Char, Acc) -> ?FNV32_HASH(Acc, Char) end, ?FNV32_INIT, binary_to_list(City)).