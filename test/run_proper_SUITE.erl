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
  Opts = [{numtests, ?iterations_per_batch},
          {max_shrinks, 10},
          nocolors,
          quiet
         ],
  Ns = [{X, 1000 * X} || X <- lists:seq(1, ?batches)],
  lists:foreach(fun({Batch, N}) ->
                  io:format("### Starting batch #~p with roughly ~p unique keys...~n", [Batch, N]),
                  case proper:quickcheck(prop_write_to_file(N), Opts) of
                    true ->
                      io:format("### Passed batch #~p!~n~n", [Batch]),
                      ok;
                    false ->
                      error(failed)
                  end
                end, Ns).

prop_write_to_file(N) ->
  ?FORALL(List,
          non_empty(?LET(TupleCount, N, generate_tuples(TupleCount))),
          begin
            Filename = "test.txt",
            UniqueKeys = lists:usort([ Str || {Str, _Float} <- List ]),
            io:format("Preparing file with ~p rows and ~p unique keys!~n", [length(List), length(UniqueKeys)]),
            prepare_file(Filename, List),
            Result = run([list_to_atom(Filename)]),
            GotKeys = maps:keys(Result),
            SentKeys = maps:keys(maps:from_list(List)),
            lists:sort(GotKeys) =:= lists:sort(SentKeys)
          end).

generate_tuples(TupleCount) ->
  L = lists:flatten([do_generate_tuples() || _ <- lists:seq(1, TupleCount)]),
  %% Shuffle the list in order to not get X cities after eachother all the time
  [ X || {_,X} <- lists:sort([ {rand:uniform(), N} || N <- L])].

do_generate_tuples() ->
  {ok, NumForString} = proper_gen:pick(integer(1, 20)),
  {ok, RandomString} = proper_gen:pick(random_string()),
  [{RandomString, random_float()} || _ <- lists:seq(1, NumForString)].

random_string() ->
  ?LET(Len, integer(1, 100),
    ?LET(Str, vector(Len, utf8_char()),
      begin
        list_to_binary(Str)
      end
    )
  ).

utf8_char() ->
  ?LET(
    Char,
    frequency([{80, integer(65, 90)},  % A-Z
               {80, integer(97, 122)}, % a-z
               {20, integer(192, 246)}, % Special characters
               {20, integer(248, 255)}  % Special characters
              ]),
    Char
  ).

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
  LkupTable = brc:find_cities(atom_to_list(File)),
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