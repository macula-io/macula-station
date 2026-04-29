#!/usr/bin/env bash
# Count per-identity processes + check pg group size + look at dht_server processes
set -eu

HOST="${1:?Usage: $0 <hostname>}"
KEY="${SSH_KEY:-$HOME/.ssh/id_hetzner}"
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$HOST"

run_eval() {
  local label="$1"; local expr="$2"
  echo "=== $label ==="
  $SSH "docker exec hecate-station bin/hecate_station eval '$expr'"
  echo
}

run_eval "Active identity count + sup children" '
  Id = hecate_station_identity_registry,
  Children = supervisor:which_children(Id),
  {{registered, lists:member(Id, registered())},
   {child_count, length(Children)},
   {sample_first_3, lists:sublist(Children, 3)}}.
'

run_eval "pg group size + members on hecate_station_facts/mesh_realm" '
  case lists:member(hecate_station_facts, pg:which_groups(hecate_station_facts)) of
    _ -> ok
  end,
  Groups = pg:which_groups(hecate_station_facts),
  Members = pg:get_members(hecate_station_facts, mesh_realm),
  {groups, Groups, member_count, length(Members)}.
'

run_eval "Process count by initial_call (top 15)" '
  Ps = erlang:processes(),
  Calls = [element(2, process_info(P, initial_call)) || P <- Ps, process_info(P, initial_call) =/= undefined],
  Counts = lists:foldl(fun(C, A) -> maps:update_with(C, fun(N) -> N+1 end, 1, A) end, #{}, Calls),
  Sorted = lists:reverse(lists:keysort(2, maps:to_list(Counts))),
  lists:sublist(Sorted, 15).
'

run_eval "Process count by registered ancestor (proc_lib dict trace)" '
  Ps = erlang:processes(),
  Tagged = lists:filtermap(fun(P) ->
    case process_info(P, dictionary) of
      {dictionary, D} ->
        case proplists:get_value(\"$initial_call\", D) of
          undefined ->
            case lists:keyfind(\"$initial_call\", 1, D) of
              {_, IC} -> {true, IC};
              _ -> false
            end;
          IC -> {true, IC}
        end;
      _ -> false
    end
  end, Ps),
  Counts = lists:foldl(fun(C, A) -> maps:update_with(C, fun(N) -> N+1 end, 1, A) end, #{}, Tagged),
  Sorted = lists:reverse(lists:keysort(2, maps:to_list(Counts))),
  lists:sublist(Sorted, 20).
'

run_eval "Memory + GC stats one fact_publisher" '
  Ps = erlang:processes(),
  M = [{element(2, process_info(P, memory)), P} || P <- Ps,
       process_info(P, memory) =/= undefined,
       case process_info(P, dictionary) of
         {dictionary, D} ->
           lists:any(fun({K,V}) -> K == \"$initial_call\" andalso element(1,V) == hecate_station_fact_publisher end, D);
         _ -> false
       end],
  case M of
    [] -> {no_publisher_found};
    _ ->
      [{Mem, P}] = lists:sublist(lists:reverse(lists:sort(M)), 1),
      process_info(P, [memory, total_heap_size, heap_size, stack_size, garbage_collection, message_queue_len, status])
  end.
'
