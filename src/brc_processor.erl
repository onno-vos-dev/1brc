
-module(brc_processor).

-export([start/1]).

start(Workers) ->
  process_flag(message_queue_data, off_heap),
  processor_loop(Workers, self(), <<>>, []).

processor_loop([], _Self, _PrevChunkRem, Acc) -> hd(Acc);
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
      MergedAcc = lists:foldl(fun(Map, A) -> merge_maps(Map, A) end, WorkerAcc, Acc),
      processor_loop(Workers -- [Worker], Self, PrevChunkRem, [MergedAcc])
  end.

select_worker([SelectedWorker | RemainingWorkers]) ->
  NewWorkers = RemainingWorkers ++ [SelectedWorker],
  {SelectedWorker, NewWorkers}.

merge_maps(Map1, Map2) ->
  maps:fold(fun(K, {Min2, Max2, MeasurementAcc2, N2}, A) when is_map_key(K, Map2) ->
                 {ok, {Min1, Max1, MeasurementAcc1, N1}} = maps:find(K, Map2),
                 A#{K => {min(Min1, Min2), max(Max1, Max2), MeasurementAcc1 + MeasurementAcc2, N1 + N2}};
               (K, {Min2, Max2, MeasurementAcc2, N2}, A) ->
                 A#{K => {Min2, Max2, MeasurementAcc2, N2}}
            end, #{}, Map1).

find_complete_chunk(Chunk) ->
  ByteSize = byte_size(Chunk),
  {Start, _Length} = binary:match(rev_binary(binary:part(Chunk, ByteSize - 150, 150)), <<"\n">>),
  CompletePart = binary:part(Chunk, 0, ByteSize - Start),
  RemPart =  binary:part(Chunk, ByteSize - Start, Start),
  {CompletePart, RemPart}.

rev_binary(Binary) ->
  Size = erlang:size(Binary)*8,
  <<X:Size/integer-little>> = Binary,
  <<X:Size/integer-big>>.