%% Thin wrapper with Elixir-compatible module name for Vor extern calls.
%% Vor's extern declaration `VorDB.FFI.function(...)` generates
%% `'Elixir.VorDB.FFI':function(...)` — this module provides that name.
-module('Elixir.VorDB.FFI').
-compile(export_all).

%% Storage
storage_put_lww(V, K, Val) -> vordb_ffi:storage_put_lww(V, K, Val).
storage_put_set(V, K, Val) -> vordb_ffi:storage_put_set(V, K, Val).
storage_put_counter(V, K, Val) -> vordb_ffi:storage_put_counter(V, K, Val).
storage_get_all_lww(V) -> vordb_ffi:storage_get_all_lww(V).
storage_get_all_sets(V) -> vordb_ffi:storage_get_all_sets(V).
storage_get_all_counters(V) -> vordb_ffi:storage_get_all_counters(V).
storage_put_all_lww(V, E) -> vordb_ffi:storage_put_all_lww(V, E).
storage_put_all_sets(V, E) -> vordb_ffi:storage_put_all_sets(V, E).
storage_put_all_counters(V, E) -> vordb_ffi:storage_put_all_counters(V, E).

%% Entry
entry_new(V, T, N) -> vordb_ffi:entry_new(V, T, N).
entry_tombstone(T, N) -> vordb_ffi:entry_tombstone(T, N).
entry_lookup(S, K) -> vordb_ffi:entry_lookup(S, K).

%% OR-Set
orset_get_or_empty(S, K) -> vordb_ffi:orset_get_or_empty(S, K).
orset_has_key(S, K) -> vordb_ffi:orset_has_key(S, K).
orset_add_element(S, E, T) -> vordb_ffi:orset_add_element(S, E, T).
orset_remove_element(S, E) -> vordb_ffi:orset_remove_element(S, E).
orset_read_elements(S) -> vordb_ffi:orset_read_elements(S).
orset_make_tag(N, T, C) -> vordb_ffi:orset_make_tag(N, T, C).
orset_merge_stores(L, R) -> vordb_ffi:orset_merge_stores(L, R).

%% Counter
counter_get_or_empty(S, K) -> vordb_ffi:counter_get_or_empty(S, K).
counter_has_key(S, K) -> vordb_ffi:counter_has_key(S, K).
counter_increment(C, N, A) -> vordb_ffi:counter_increment(C, N, A).
counter_decrement(C, N, A) -> vordb_ffi:counter_decrement(C, N, A).
counter_value(C) -> vordb_ffi:counter_value(C).
counter_merge_stores(L, R) -> vordb_ffi:counter_merge_stores(L, R).

%% Dirty tracker
dirty_mark(V, T, K) -> vordb_ffi:dirty_mark(V, T, K).
dirty_mark_keys(V, T, K) -> vordb_ffi:dirty_mark_keys(V, T, K).
dirty_confirm_ack(V, P, S) -> vordb_ffi:dirty_confirm_ack(V, P, S).

%% Map utils
map_select_keys(S, K) -> vordb_ffi:map_select_keys(S, K).

%% Gossip
gossip_send_vnode_deltas(N, V) -> vordb_ffi:gossip_send_vnode_deltas(N, V).

%% Time
system_time_ms() -> vordb_ffi:system_time_ms().
