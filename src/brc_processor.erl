
-module(brc_processor).

-export([start/1]).

start(Workers) ->
  process_flag(message_queue_data, off_heap),
  processor_loop(Workers, self(), <<>>, #{}).

processor_loop([], _Self, _PrevChunkRem, Acc) ->
  Acc;
processor_loop(Workers, Self, PrevChunkRem, Acc) ->
  receive
    {chunk, Chunk} ->
      {CompleteChunk, RemChunk} = find_complete_chunk(<<PrevChunkRem/binary, Chunk/binary>>),
      {WorkerPid, NewWorkers} = select_worker(Workers),
      WorkerPid ! {chunk, CompleteChunk},
      processor_loop(NewWorkers, Self, RemChunk, Acc);
    done ->
      [ Worker ! {self(), no_more_work} || Worker <- Workers ],
      processor_loop(Workers, Self, PrevChunkRem, Acc);
    {Worker, worker_done, WorkerAcc} ->
      MergedAcc = merge_maps(WorkerAcc, Acc),
      processor_loop(Workers -- [Worker], Self, PrevChunkRem, MergedAcc)
  end.

select_worker([SelectedWorker | RemainingWorkers]) ->
  NewWorkers = RemainingWorkers ++ [SelectedWorker],
  {SelectedWorker, NewWorkers}.

merge_maps(Map1, Map2) ->
  Cities = maps:keys(Map1) ++ maps:keys(Map2),
  lists:foldl(fun(City, Acc) when is_map_key(City, Map1) andalso is_map_key(City, Map2) ->
                   #{City := {Min1, Max1, MAcc1, N1}} = Map1,
                   #{City := {Min2, Max2, MAcc2, N2}} = Map2,
                   Acc#{City => {min(Min1, Min2), max(Max1, Max2), MAcc1 + MAcc2, N1 + N2}};
                 (City, Acc) when is_map_key(City, Map1) andalso not is_map_key(City, Map2) ->
                   #{City := {Min1, Max1, MAcc1, N1}} = Map1,
                   Acc#{City => {Min1, Max1, MAcc1, N1}};
                 (City, Acc) when not is_map_key(City, Map1) andalso is_map_key(City, Map2) ->
                   #{City := {Min2, Max2, MAcc2, N2}} = Map2,
                   Acc#{City => {Min2, Max2, MAcc2, N2}}
                end, #{}, Cities).

find_complete_chunk(Chunk) ->
  ByteSize = byte_size(Chunk),
  {Start, _Length} =
    case ByteSize >= 250 of
      true ->
        binary:match(rev_binary(binary:part(Chunk, ByteSize - 250, 250)), <<"\n">>);
      false ->
        binary:match(rev_binary(Chunk), <<"\n">>)
    end,
  CompletePart = binary:part(Chunk, 0, ByteSize - Start),
  RemPart =  binary:part(Chunk, ByteSize - Start, Start),
  {CompletePart, RemPart}.

rev_binary(Binary) ->
  Size = erlang:size(Binary)*8,
  <<X:Size/integer-little>> = Binary,
  <<X:Size/integer-big>>.