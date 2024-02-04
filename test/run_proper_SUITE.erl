-module(run_proper_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-export([ all/0,
          run_test/1
        ]).

-define(iterations_per_batch, 2).
-define(batches, 25).

all() ->
  [ run_test ].

run_test(_Config) ->
  {ok, CitiesBin} = file:read_file(code:lib_dir(brc) ++ "/test/" ++ "cities_with_population_1000.txt"),
  CitiesL = binary:split(CitiesBin, <<"\n">>, [global]),
  CitiesMap = maps:from_list(lists:zip(lists:seq(1, length(CitiesL)), CitiesL)),
  ProperOpts = [{numtests, ?iterations_per_batch},
          {max_shrinks, 0},
          nocolors
         ],
  Ns = [{X, 1000 * X} || X <- lists:seq(1, ?batches)],
  lists:foreach(fun({Batch, N}) ->
                  io:format("### Starting batch #~p with ~p unique keys...~n", [Batch, N]),
                  case proper:quickcheck(prop_write_to_file(CitiesMap, N), ProperOpts) of
                    true ->
                      io:format("### Passed batch #~p!~n~n", [Batch]),
                      ok;
                    false ->
                      error(failed)
                  end
                end, Ns).

prop_write_to_file(CitiesMap, N) ->
  ?FORALL(Floats,
          non_empty(?LET(Count, N, generate_floats(Count))),
          begin
            Filename = random_filename(),
            SelectedCitiesMap = maps:from_list([ {X, maps:get(rand:uniform(maps:size(CitiesMap)), CitiesMap)} || X <- lists:seq(1, N) ]),
            List = [ {maps:get(rand:uniform(maps:size(SelectedCitiesMap)), SelectedCitiesMap), Float} || Float <- Floats ],
            io:format("Preparing file with ~p rows and ~p unique keys!~n", [length(List), N]),
            prepare_file(Filename, List),
            Result = run([list_to_atom(Filename)]),
            GotKeys = maps:keys(Result),
            SentKeys = maps:keys(maps:from_list(List)),
            ?WHENFAIL(io:format("Filename for repeat: ~p~n", [Filename]),
                      lists:sort(GotKeys) =:= lists:sort(SentKeys))
          end).

random_filename() ->
  <<X:32, Y:32, Z:32>> = crypto:strong_rand_bytes(12),
  "test_" ++ integer_to_list(X) ++ integer_to_list(Y) ++ integer_to_list(Z) ++ ".txt".

generate_floats(Count) ->
  lists:flatten([do_generate_floats() || _ <- lists:seq(1, Count)]).

do_generate_floats() ->
  {ok, NumForString} = proper_gen:pick(integer(1, 20)),
  [random_float() || _ <- lists:seq(1, NumForString)].

random_float() ->
  ?LET(X, float(-99.9, 99.9),
    begin
      list_to_float(io_lib:format("~.1f", [X]))
    end).

prepare_file(Filename, List) ->
  {ok, File} = file:open(Filename, [write]),
  lists:foreach(fun({Str, Flt}) ->
                  Data = io_lib:format("~s;~p\n", [Str, Flt]),
                  ok = file:write(File, Data)
                end, List),
  file:close(File).

run([File]) ->
  %% Just read all the cities into LkupTable. PropEr generates some really weird
  %% City;Float pairs and it's not representative of what the Java generator does
  LkupTable = brc:find_cities(atom_to_list(File), filelib:file_size(File)),
  Workers = brc_workers:spawn_workers(erlang:system_info(logical_processors)),
  {Pid, Ref} = erlang:spawn_monitor(fun() -> exit({normal, brc_processor:start(Workers)}) end),
  brc_reader:start(File, Pid),
  receive
    {'DOWN', Ref, process, Pid, {normal, Result}} ->
      [ exit(P, kill) || P <- Workers ],
      maps:fold(fun(K, V, Acc) -> Acc#{maps:get(K, LkupTable) => V} end, #{}, Result);
    {'DOWN', Ref, process, Pid, Reason} ->
      error({worker_died, Reason})
  end.