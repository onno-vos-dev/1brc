-module(brc).

-export([run/1, find_cities/2]).

-include("hash.hrl").

run([File]) ->
  Workers = brc_workers:spawn_workers(erlang:system_info(logical_processors)),
  {Pid, Ref} = erlang:spawn_monitor(fun() -> exit({normal, brc_processor:start(Workers)}) end),
  brc_reader:start(File, Pid),
  LkupTable = find_cities(atom_to_list(File), 1024*1024*2), %% 2MB should be enough :-)
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
  create_lookup_table(Data, #{}).

create_lookup_table(Bin, State) ->
  create_lookup_table(Bin, State, {<<>>, ?INIT}).

create_lookup_table(<<C:8, $;:8, Rest/binary>>, State, {Raw, Acc}) ->
  do_create_lookup_table(Rest, State, {<<Raw/binary, C>>, ?HASH(Acc, C)});
create_lookup_table(<<C:16, $;:8, Rest/binary>> = Bin, State, {Raw, Acc}) ->
  <<C1:8, C2:8, _/binary>> = Bin,
  do_create_lookup_table(Rest, State, {<<Raw/binary, C1, C2>>, ?HASH(Acc, C)});
create_lookup_table(<<C:24, $;:8, Rest/binary>> = Bin, State, {Raw, Acc}) ->
  <<C1:8, C2:8, C3:8, _/binary>> = Bin,
  do_create_lookup_table(Rest, State, {<<Raw/binary, C1, C2, C3>>, ?HASH(Acc, C)});
create_lookup_table(<<C:24, Rest/binary>> = Bin, State, {Raw, Acc}) ->
  <<C1:8, C2:8, C3:8, _/binary>> = Bin,
  create_lookup_table(Rest, State, {<<Raw/binary, C1, C2, C3>>, ?HASH(Acc, C)});
create_lookup_table(_, State, _) ->
State.

do_create_lookup_table(<<_:40, $\n:8, Rest/binary>>, State, {CityRaw, City}) ->
  create_lookup_table(Rest, State#{City => CityRaw}, {<<>>, ?INIT});
do_create_lookup_table(<<_:32, $\n:8, Rest/binary>>, State, {CityRaw, City}) ->
  create_lookup_table(Rest, State#{City => CityRaw}, {<<>>, ?INIT});
do_create_lookup_table(<<_:24, $\n:8, Rest/binary>>, State, {CityRaw, City}) ->
  create_lookup_table(Rest, State#{City => CityRaw}, {<<>>, ?INIT});
do_create_lookup_table(_, State, {CityRaw, City}) ->
  State#{City => CityRaw}.

-ifdef(EUNIT).

-include_lib("eunit/include/eunit.hrl").

all_cities_hasheable_test() ->
  {CitiesL, Map} = create_lookup_table_from_all_cities(),
  Keys = maps:keys(Map),
  io:format("Smallest hash: ~p Biggest hash: ~p~n", [lists:min(Keys), lists:max(Keys)]),
  io:format("Missing: ~p~n", [CitiesL -- maps:values(Map)]),
  ?assertEqual(length(CitiesL), maps:size(Map)).

create_lookup_table_from_all_cities() ->
  {ok, CitiesBin} = file:read_file(code:lib_dir(brc) ++ "/test/" ++ "cities_with_population_1000.txt"),
  CitiesL = binary:split(CitiesBin, <<"\n">>, [global]),
  {CitiesL, lists:foldl(fun(City, Acc) -> create_lookup_table(<<City/binary, ";">>, Acc, {<<>>, ?INIT}) end, #{}, CitiesL)}.

-endif.