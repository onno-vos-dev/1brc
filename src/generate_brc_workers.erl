-module(generate_brc_workers).

-export([generate_and_compile/1]).

generate_and_compile(LkupTable) ->
  Start = init_module(),
  ProcessLines = process_lines(LkupTable),
  End = remaining_parts_of_module(),
  Full = <<Start/binary, ProcessLines/binary, End/binary>>,
  {ok, Tokens, _} = erl_scan:string(binary_to_list(Full)),
  {_, ParsedForms} = lists:foldl(fun(Token, {TokenAcc, FormsAcc}) ->
                                   case Token of
                                     {dot,_} ->
                                       FormTokens = lists:reverse([Token | TokenAcc]),
                                       {ok, Forms} = erl_parse:parse_form(FormTokens),
                                       {[], [Forms | FormsAcc]};
                                     Token ->
                                       {[Token | TokenAcc], FormsAcc}
                                   end
                                 end, {[], []}, Tokens),
  {ok, brc_workers, Bin} = compile:forms(lists:reverse(ParsedForms)),
  {module, brc_workers} = code:load_binary(brc_workers, "nofile", Bin).

init_module() ->
  <<"-module(brc_workers).\n",
    "\n",
    "-export([spawn_workers/1]).\n",
    "\n",
    "spawn_workers(N) ->\n",
    "  Options = [link,\n",
    "             {min_heap_size, 1024*1024},\n",
    "             {min_bin_vheap_size, 1024*1024},\n",
    "             {max_heap_size, (1 bsl 59) -1}\n",
    "            ],\n",
    "  spawn_workers(N, Options, []).\n",
    "\n",
    "spawn_workers(0, _Options, Acc) -> Acc;\n",
    "spawn_workers(N, Options, Acc) ->\n",
    "  Pid = erlang:spawn_opt(fun() ->\n",
    "                            worker()\n",
    "                          end,\n",
    "                          Options),\n",
    "  spawn_workers(N - 1, Options, [Pid | Acc]).\n",
    "\n",
    "worker() ->\n",
    "  process_flag(message_queue_data, off_heap),\n",
    "  process_flag(priority, high),\n",
    "  receive\n",
    "    {chunk, Chunk} ->\n",
    "      process_lines(Chunk),\n",
    "      worker();\n",
    "    {Parent, no_more_work} ->\n",
    "      Parent ! {self(), worker_done, maps:from_list(get())}\n",
    "    end.\n"
    "\n">>.

process_lines(LkupTable) ->
  Bin0 =
    lists:foldl(fun({Hash, City}, Acc) ->
                   <<Acc/binary,
                     "process_lines(<<\"", City/binary, "\", $;:8, Rest/binary>>) ->\n"
                     "  do_process_line(Rest, ", (integer_to_binary(Hash))/binary, ");\n">>
                end,
                <<>>,
                maps:to_list(LkupTable)),
  <<Bin0/binary, "process_lines(<<>>) ->\n  ok.\n\n">>.

remaining_parts_of_module() ->
  <<"%% Very specialized float-parser for floats with a single fractional\n"
    "%% digit, and returns the result as an integer * 10.\n"
    "do_process_line(<<$-, A:8, B:8, $., C:8, $\\n:8, Rest/binary>>, City) ->\n"
    "  add_to_state(Rest, City, -1 * ((A - $0) * 100 + (B - $0) * 10 + (C - $0)));\n"
    "do_process_line(<<$-, B:8, $., C:8, $\\n:8, Rest/binary>>, City) ->\n"
    "  add_to_state(Rest, City, -1 * ((B - $0) * 10 + (C - $0)));\n"
    "do_process_line(<<A:8, B:8, $., C:8, $\\n:8, Rest/binary>>, City) ->\n"
    "  add_to_state(Rest, City, (A - $0) * 100 + (B - $0) * 10 + (C - $0));\n"
    "do_process_line(<<B:8, $., C:8, $\\n:8, Rest/binary>>, City) ->\n"
    "  add_to_state(Rest, City, (B - $0) * 10 + (C - $0)).\n"
    "\n"
    "add_to_state(<<Rest/binary>>, City, Measurement) ->\n"
    "  case get(City) of\n"
    "    undefined ->\n"
    "      put(City, {Measurement, Measurement, Measurement, 1}),\n"
    "      process_lines(Rest);\n"
    "    {Min, Max, MeasurementAcc, N} ->\n"
    "      put(City, {min(Min, Measurement), max(Max, Measurement), MeasurementAcc + Measurement, N + 1}),\n"
    "      process_lines(Rest)\n"
    "  end.\n">>.