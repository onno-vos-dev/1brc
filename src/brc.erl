-module(brc).

-export([run/1, find_cities/2]).

run([File]) ->
  LkupTable = find_cities(atom_to_list(File), 1024*1024*2), %% 2MB should be enough :-)
  generate_brc_workers:generate_and_compile(LkupTable),
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
  "{" ++ lists:join(", ", lists:sort(Fmts)) ++ "}".

round_back(IntFloat) ->
  list_to_float(io_lib:format("~.1f", [IntFloat / 10])).

find_cities(File, Size) ->
  {ok, FD} = prim_file:open(File, [read, binary, raw, read_ahead]),
  {ok, Data} = prim_file:read(FD, Size),
  Cities = create_lookup_table(Data, []),
  maps:from_list(lists:zip(lists:seq(1, length(Cities)), Cities)).

create_lookup_table(Bin, State) ->
  Cities0 = create_lookup_table(Bin, State, <<>>),
  SortedCities = lists:foldl(fun(City, Acc) -> maps:update_with(City, fun(V) -> V + 1 end, 1, Acc) end, #{}, Cities0),
  {Cities, _} = lists:unzip(lists:reverse(lists:keysort(2, maps:to_list(SortedCities)))),
  Cities.

create_lookup_table(<<C:8, $;:8, Rest/binary>>, State, City) ->
  do_create_lookup_table(Rest, State, <<City/binary, C>>);
create_lookup_table(<<C:8, Rest/binary>>, State, City) ->
  create_lookup_table(Rest, State, <<City/binary, C>>);
create_lookup_table(_, State, _) ->
  State.

do_create_lookup_table(<<_:40, $\n:8, Rest/binary>>, State, CityRaw) ->
  create_lookup_table(Rest, [CityRaw | State], <<>>);
do_create_lookup_table(<<_:32, $\n:8, Rest/binary>>, State, CityRaw) ->
  create_lookup_table(Rest, [CityRaw | State], <<>>);
do_create_lookup_table(<<_:24, $\n:8, Rest/binary>>, State, CityRaw) ->
  create_lookup_table(Rest, [CityRaw | State], <<>>);
do_create_lookup_table(_, State, CityRaw) ->
  [CityRaw | State].