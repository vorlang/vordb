-module(vordb_ffi).

%% Vor agent extern bridge — all Vor extern calls route through here.
%% Storage lifecycle managed via a named gen_server (vordb_storage).

-behaviour(gen_server).
-export([
    %% Storage lifecycle
    storage_start/1, storage_stop/0,
    %% Storage ops (called by Vor agent via Erlang.vordb_ffi.*)
    storage_put_lww/3, storage_put_set/3, storage_put_counter/3,
    storage_get_all_lww/1, storage_get_all_sets/1, storage_get_all_counters/1,
    storage_put_all_lww/2, storage_put_all_sets/2, storage_put_all_counters/2,
    %% Entry helpers (called by Vor agent)
    entry_new/3, entry_tombstone/2, entry_lookup/2,
    %% OR-Set helpers (called by Vor agent)
    orset_get_or_empty/2, orset_has_key/2, orset_add_element/3,
    orset_remove_element/2, orset_read_elements/1, orset_make_tag/3,
    orset_merge_stores/2,
    %% Counter helpers (called by Vor agent)
    counter_get_or_empty/2, counter_has_key/2,
    counter_increment/3, counter_decrement/3, counter_value/1,
    counter_merge_stores/2,
    %% Dirty tracker (called by Vor agent)
    dirty_mark/3, dirty_mark_keys/3, dirty_confirm_ack/3,
    %% Map utils (called by Vor agent)
    map_select_keys/2,
    %% Gossip (called by Vor agent every block)
    gossip_send_vnode_deltas/2,
    gossip_send_full_sync/0,
    %% Agent call/cast
    call_agent/2, cast_agent/2,
    %% Ring
    ring_from_binary/1,
    %% Storage partition ops (for handoff)
    storage_iterate_partition/3, storage_delete_partition/1,
    storage_count_partition_keys/1,
    %% General FFI
    system_time_ms/0, phash2/2,
    node_self/0, node_list/0, node_connect/1, erpc_cast_vnode/3,
    %% Vor message constructors (called by Gleam public API)
    make_put_msg/2, make_get_msg/1, make_delete_msg/1,
    make_set_add_msg/2, make_set_remove_msg/2, make_set_members_msg/1,
    make_counter_increment_msg/2, make_counter_decrement_msg/2, make_counter_value_msg/1,
    make_get_stores_msg/0,
    %% Registry
    registry_start/1, registry_lookup_vnode/1,
    %% Vnode start
    start_kv_store/4,
    %% Gen_server callbacks
    init/1, handle_call/3, handle_cast/2, terminate/2
]).

%% ===== Storage Lifecycle (Column Family based) =====

storage_start(DataDir) ->
    filelib:ensure_dir(filename:join(DataDir, "dummy")),
    gen_server:start_link({local, vordb_storage}, ?MODULE, DataDir, []).

storage_stop() ->
    gen_server:stop(vordb_storage).

%% ===== Gen_server callbacks =====

init(DataDir) ->
    DirStr = case is_binary(DataDir) of
        true -> binary_to_list(DataDir);
        false -> DataDir
    end,
    DbOpts = [{create_if_missing, true}, {write_buffer_size, 64 * 1024 * 1024}, {max_open_files, 1000}],
    CfOpts = [{write_buffer_size, 4 * 1024 * 1024}, {max_write_buffer_number, 2}],

    %% List existing CFs (or default for fresh db)
    CfNames = case rocksdb:list_column_families(DirStr, DbOpts) of
        {ok, Names} -> Names;
        {error, _} -> ["default"]
    end,

    %% Open with all existing CFs
    CfSpecs = [{N, CfOpts} || N <- CfNames],
    case rocksdb:open_with_cf(DirStr, DbOpts, CfSpecs) of
        {ok, Db, CfHandles} ->
            %% Build name→handle map
            HandleMap = maps:from_list(lists:zip(CfNames, CfHandles)),
            DefaultCf = maps:get("default", HandleMap),
            {ok, #{db => Db, cf_handles => HandleMap, default_cf => DefaultCf, path => DirStr}};
        {error, Reason} ->
            {stop, {rocksdb_open_failed, Reason}}
    end.

%% CF-based put: writes to partition's column family
handle_call({cf_put, Partition, Key, Value}, _From, #{db := Db, cf_handles := CfH} = State) ->
    CfName = cf_name(Partition),
    case maps:get(CfName, CfH, undefined) of
        undefined -> {reply, {error, unknown_partition}, State};
        CfHandle ->
            Encoded = erlang:term_to_binary(Value),
            case rocksdb:put(Db, CfHandle, Key, Encoded, []) of
                ok -> {reply, ok, State};
                {error, R} -> {reply, {error, R}, State}
            end
    end;

%% CF-based get
handle_call({cf_get, Partition, Key}, _From, #{db := Db, cf_handles := CfH} = State) ->
    CfName = cf_name(Partition),
    case maps:get(CfName, CfH, undefined) of
        undefined -> {reply, not_found, State};
        CfHandle ->
            case rocksdb:get(Db, CfHandle, Key, []) of
                {ok, Bin} -> {reply, {ok, erlang:binary_to_term(Bin)}, State};
                not_found -> {reply, not_found, State};
                {error, R} -> {reply, {error, R}, State}
            end
    end;

%% CF-based get_all by type prefix within a partition's CF
handle_call({cf_get_all, Partition, TypePrefix}, _From, #{db := Db, cf_handles := CfH} = State) ->
    CfName = cf_name(Partition),
    case maps:get(CfName, CfH, undefined) of
        undefined -> {reply, #{}, State};
        CfHandle ->
            {ok, Iter} = rocksdb:iterator(Db, CfHandle, []),
            PrefixBin = <<TypePrefix/binary, ":">>,
            PLen = byte_size(PrefixBin),
            Entries = iterate_cf_prefix(Iter, rocksdb:iterator_move(Iter, first), PrefixBin, PLen, #{}),
            rocksdb:iterator_close(Iter),
            {reply, Entries, State}
    end;

%% CF-based batch put within a partition's CF
handle_call({cf_put_all, Partition, TypePrefix, Entries}, _From, #{db := Db, cf_handles := CfH} = State) ->
    CfName = cf_name(Partition),
    case maps:get(CfName, CfH, undefined) of
        undefined -> {reply, {error, unknown_partition}, State};
        CfHandle ->
            Batch = maps:fold(fun(K, V, Acc) ->
                FullKey = <<TypePrefix/binary, ":", K/binary>>,
                [{put, CfHandle, FullKey, erlang:term_to_binary(V)} | Acc]
            end, [], Entries),
            case rocksdb:write(Db, Batch, []) of
                ok -> {reply, ok, State};
                {error, R} -> {reply, {error, R}, State}
            end
    end;

%% Delete all data in a partition's CF
handle_call({cf_delete_partition, Partition}, _From, #{db := Db, cf_handles := CfH} = State) ->
    CfName = cf_name(Partition),
    case maps:get(CfName, CfH, undefined) of
        undefined -> {reply, ok, State};
        CfHandle ->
            {ok, Iter} = rocksdb:iterator(Db, CfHandle, []),
            delete_all_in_cf(Db, CfHandle, Iter, rocksdb:iterator_move(Iter, first)),
            rocksdb:iterator_close(Iter),
            {reply, ok, State}
    end;

%% Create column family for a new partition
handle_call({cf_create, Partition}, _From, #{db := Db, cf_handles := CfH} = State) ->
    CfName = cf_name(Partition),
    case maps:get(CfName, CfH, undefined) of
        undefined ->
            CfOpts = [{write_buffer_size, 4 * 1024 * 1024}, {max_write_buffer_number, 2}],
            case rocksdb:create_column_family(Db, CfName, CfOpts) of
                {ok, CfHandle} ->
                    NewCfH = maps:put(CfName, CfHandle, CfH),
                    {reply, ok, State#{cf_handles := NewCfH}};
                {error, R} -> {reply, {error, R}, State}
            end;
        _ -> {reply, ok, State}  %% Already exists
    end;

%% Drop column family
handle_call({cf_drop, Partition}, _From, #{cf_handles := CfH} = State) ->
    CfName = cf_name(Partition),
    case maps:get(CfName, CfH, undefined) of
        undefined -> {reply, ok, State};
        CfHandle ->
            catch rocksdb:drop_column_family(CfHandle),
            NewCfH = maps:remove(CfName, CfH),
            {reply, ok, State#{cf_handles := NewCfH}}
    end;

%% Metadata in default CF
handle_call({meta_put, Key, Value}, _From, #{db := Db, default_cf := Dcf} = State) ->
    case rocksdb:put(Db, Dcf, Key, Value, []) of
        ok -> {reply, ok, State};
        {error, R} -> {reply, {error, R}, State}
    end;

handle_call({meta_get, Key}, _From, #{db := Db, default_cf := Dcf} = State) ->
    case rocksdb:get(Db, Dcf, Key, []) of
        {ok, Bin} -> {reply, {ok, Bin}, State};
        not_found -> {reply, not_found, State};
        {error, R} -> {reply, {error, R}, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #{db := Db}) ->
    rocksdb:close(Db).

%% CF iteration helpers
iterate_cf_prefix(Iter, {ok, Key, Value}, Prefix, PLen, Acc) ->
    case Key of
        <<Prefix:PLen/binary, Rest/binary>> ->
            Entry = erlang:binary_to_term(Value),
            iterate_cf_prefix(Iter, rocksdb:iterator_move(Iter, next), Prefix, PLen, maps:put(Rest, Entry, Acc));
        _ ->
            iterate_cf_prefix(Iter, rocksdb:iterator_move(Iter, next), Prefix, PLen, Acc)
    end;
iterate_cf_prefix(_Iter, {error, invalid_iterator}, _Prefix, _PLen, Acc) -> Acc.

delete_all_in_cf(Db, CfHandle, Iter, {ok, Key, _Value}) ->
    rocksdb:delete(Db, CfHandle, Key, []),
    delete_all_in_cf(Db, CfHandle, Iter, rocksdb:iterator_move(Iter, next));
delete_all_in_cf(_Db, _CfHandle, _Iter, {error, invalid_iterator}) -> ok.

iterate_prefix(Iter, {ok, Key, Value}, Prefix, PrefixLen, Acc) ->
    NewAcc = case Key of
        <<Prefix:PrefixLen/binary, Rest/binary>> ->
            maps:put(Rest, erlang:binary_to_term(Value), Acc);
        _ -> Acc
    end,
    iterate_prefix(Iter, rocksdb:iterator_move(Iter, next), Prefix, PrefixLen, NewAcc);
iterate_prefix(_Iter, {error, invalid_iterator}, _Prefix, _PrefixLen, Acc) ->
    Acc.

%% ===== Storage API (Vor extern targets) =====

cf_name(Partition) ->
    lists:flatten("partition_" ++ string:pad(integer_to_list(Partition), 3, leading, $0)).

storage_put_lww(VnodeId, Key, Value) ->
    gen_server:call(vordb_storage, {cf_put, VnodeId, <<"lww:", Key/binary>>, Value}).
storage_put_set(VnodeId, Key, Value) ->
    gen_server:call(vordb_storage, {cf_put, VnodeId, <<"set:", Key/binary>>, Value}).
storage_put_counter(VnodeId, Key, Value) ->
    gen_server:call(vordb_storage, {cf_put, VnodeId, <<"counter:", Key/binary>>, Value}).

storage_get_all_lww(VnodeId) ->
    gen_server:call(vordb_storage, {cf_get_all, VnodeId, <<"lww">>}).
storage_get_all_sets(VnodeId) ->
    gen_server:call(vordb_storage, {cf_get_all, VnodeId, <<"set">>}).
storage_get_all_counters(VnodeId) ->
    gen_server:call(vordb_storage, {cf_get_all, VnodeId, <<"counter">>}).

storage_put_all_lww(VnodeId, Entries) ->
    gen_server:call(vordb_storage, {cf_put_all, VnodeId, <<"lww">>, Entries}).
storage_put_all_sets(VnodeId, Entries) ->
    gen_server:call(vordb_storage, {cf_put_all, VnodeId, <<"set">>, Entries}).
storage_put_all_counters(VnodeId, Entries) ->
    gen_server:call(vordb_storage, {cf_put_all, VnodeId, <<"counter">>, Entries}).

%% ===== Entry helpers =====

entry_new(Value, Timestamp, NodeId) ->
    #{value => Value, timestamp => Timestamp, node_id => NodeId}.

entry_tombstone(Timestamp, NodeId) ->
    #{value => '__tombstone__', timestamp => Timestamp, node_id => NodeId}.

entry_lookup(Store, Key) ->
    case maps:get(Key, Store, not_found) of
        not_found -> #{val => none, found => false};
        %% Gleam tagged tuple format
        {lww_entry, V, _T, _N} -> #{val => V, found => true};
        {tombstone, _T, _N} -> #{val => none, found => false};
        %% Legacy plain map format
        #{value := '__tombstone__'} -> #{val => none, found => false};
        #{value := V} -> #{val => V, found => true};
        _ -> #{val => none, found => false}
    end.

%% ===== OR-Set helpers =====

orset_empty() -> 'vordb@or_set':empty().

orset_get_or_empty(Store, Key) ->
    'vordb@or_set':get_or_empty(Store, Key).

orset_has_key(Store, Key) ->
    'vordb@or_set':has_key(Store, Key).

orset_add_element(SetState, Element, NodeId) ->
    'vordb@or_set':add_element(SetState, Element, NodeId).

orset_remove_element(SetState, Element) ->
    'vordb@or_set':remove_element(SetState, Element).

orset_read_elements(SetState) ->
    'vordb@or_set':read_elements(SetState).

orset_make_tag(NodeId, _Timestamp, _Counter) ->
    %% Deprecated — ORSWOT doesn't use external tags. Returns node_id for compatibility.
    NodeId.

orset_merge_stores(Local, Remote) ->
    'vordb@or_set':merge_stores(Local, Remote).

%% ===== Counter helpers =====

counter_empty() -> #{p => #{}, n => #{}}.

counter_get_or_empty(Store, Key) ->
    maps:get(Key, Store, counter_empty()).

counter_has_key(Store, Key) ->
    maps:is_key(Key, Store).

counter_increment(Counter, NodeId, Amount) ->
    P = maps:get(p, Counter),
    Current = maps:get(NodeId, P, 0),
    Counter#{p := maps:put(NodeId, Current + Amount, P)}.

counter_decrement(Counter, NodeId, Amount) ->
    N = maps:get(n, Counter),
    Current = maps:get(NodeId, N, 0),
    Counter#{n := maps:put(NodeId, Current + Amount, N)}.

counter_value(Counter) ->
    PSum = lists:sum(maps:values(maps:get(p, Counter))),
    NSum = lists:sum(maps:values(maps:get(n, Counter))),
    PSum - NSum.

counter_merge_stores(Local, Remote) ->
    maps:merge_with(fun(_K, L, R) -> counter_merge(L, R) end, Local, Remote).

counter_merge(Local, Remote) ->
    #{
        p => maps:merge_with(fun(_K, A, B) -> max(A, B) end, maps:get(p, Local), maps:get(p, Remote)),
        n => maps:merge_with(fun(_K, A, B) -> max(A, B) end, maps:get(n, Local), maps:get(n, Remote))
    }.

%% ===== Dirty Tracker =====

dirty_mark(VnodeId, Type, Key) ->
    vordb_dirty_tracker:mark_dirty(VnodeId, Type, Key).

dirty_mark_keys(VnodeId, Type, Keys) ->
    vordb_dirty_tracker:mark_dirty_keys(VnodeId, Type, Keys).

dirty_confirm_ack(VnodeId, Peer, Seq) ->
    vordb_dirty_tracker:confirm_ack(VnodeId, Peer, Seq).

%% ===== Map Utils =====

map_select_keys(Source, Keys) ->
    maps:with(Keys, Source).

%% ===== Gossip =====

gossip_send_vnode_deltas(_NodeId, VnodeId) ->
    %% Get replica peers for this partition from ring (scoped gossip)
    Peers = case whereis(vordb_ring_manager) of
        undefined -> nodes();  %% Fallback if ring not available
        _ ->
            Ring = vordb_ring_manager:get_ring(),
            SelfNode = vordb_ring_manager:self_node(),
            PrefList = 'vordb@ring':preference_list(Ring, VnodeId),
            %% Filter to remote peers only (exclude self, convert to node atoms)
            [binary_to_atom(N) || N <- PrefList, N =/= SelfNode]
    end,
    lists:foreach(fun(Peer) ->
        {Seq, Deltas} = vordb_dirty_tracker:take_deltas(VnodeId, Peer),
        case Seq > 0 of
            true ->
                send_type_deltas(Peer, VnodeId, lww, maps:get(lww, Deltas)),
                send_type_deltas(Peer, VnodeId, set, maps:get(set, Deltas)),
                send_type_deltas(Peer, VnodeId, counter, maps:get(counter, Deltas));
            false -> ok
        end
    end, Peers),
    ok.

send_type_deltas(_Peer, _VnodeId, _Type, []) -> ok;
send_type_deltas(Peer, VnodeId, lww, Keys) ->
    case registry_lookup_vnode(VnodeId) of
        {ok, Pid} ->
            {lww_entries, #{entries := Entries}} = gen_server:call(Pid, {get_lww_entries, #{keys => Keys}}),
            case maps:size(Entries) > 0 of
                true -> erpc_cast_vnode(Peer, VnodeId, {lww_sync, #{remote_lww_store => Entries}});
                false -> ok
            end;
        _ -> ok
    end;
send_type_deltas(Peer, VnodeId, set, Keys) ->
    case registry_lookup_vnode(VnodeId) of
        {ok, Pid} ->
            {set_entries, #{entries := Entries}} = gen_server:call(Pid, {get_set_entries, #{keys => Keys}}),
            case maps:size(Entries) > 0 of
                true -> erpc_cast_vnode(Peer, VnodeId, {set_sync, #{remote_set_store => Entries}});
                false -> ok
            end;
        _ -> ok
    end;
send_type_deltas(Peer, VnodeId, counter, Keys) ->
    case registry_lookup_vnode(VnodeId) of
        {ok, Pid} ->
            {counter_entries, #{entries := Entries}} = gen_server:call(Pid, {get_counter_entries, #{keys => Keys}}),
            case maps:size(Entries) > 0 of
                true -> erpc_cast_vnode(Peer, VnodeId, {counter_sync, #{remote_counter_store => Entries}});
                false -> ok
            end;
        _ -> ok
    end.

%% ===== General FFI =====

system_time_ms() -> erlang:system_time(millisecond).

%% Storage partition operations (for handoff)

storage_iterate_partition(Partition, ChunkSize, Callback) ->
    Lww = storage_get_all_lww(Partition),
    Sets = storage_get_all_sets(Partition),
    Counters = storage_get_all_counters(Partition),
    AllEntries = maps:to_list(Lww) ++ maps:to_list(Sets) ++ maps:to_list(Counters),
    send_chunks(AllEntries, ChunkSize, Partition, Callback).

send_chunks([], _ChunkSize, _Partition, _Callback) -> ok;
send_chunks(Entries, ChunkSize, Partition, Callback) ->
    {Chunk, Rest} = case length(Entries) > ChunkSize of
        true -> lists:split(ChunkSize, Entries);
        false -> {Entries, []}
    end,
    %% Build chunk data map per type
    LwwMap = maps:from_list([{K, V} || {K, V} <- Chunk, is_map(V), maps:is_key(value, V)]),
    SetMap = maps:from_list([{K, V} || {K, V} <- Chunk, is_tuple(V), element(1, V) =:= or_set_state]),
    CounterMap = maps:from_list([{K, V} || {K, V} <- Chunk, is_tuple(V), element(1, V) =:= pn_counter]),
    ChunkData = #{lww => LwwMap, sets => SetMap, counters => CounterMap},
    Callback(ChunkData),
    send_chunks(Rest, ChunkSize, Partition, Callback).

storage_delete_partition(Partition) ->
    gen_server:call(vordb_storage, {cf_delete_partition, Partition}).

storage_count_partition_keys(Partition) ->
    Lww = storage_get_all_lww(Partition),
    Sets = storage_get_all_sets(Partition),
    Counters = storage_get_all_counters(Partition),
    maps:size(Lww) + maps:size(Sets) + maps:size(Counters).

ring_from_binary(Bin) ->
    try {ok, erlang:binary_to_term(Bin)}
    catch _:_ -> {error, nil}
    end.

phash2(Key, Range) -> erlang:phash2(Key, Range).

node_self() -> node().
node_list() -> nodes().

node_connect(Node) when is_atom(Node) ->
    net_kernel:connect_node(Node);
node_connect(NodeBin) when is_binary(NodeBin) ->
    net_kernel:connect_node(binary_to_atom(NodeBin)).

erpc_cast_vnode(Peer, VnodeId, Message) ->
    erpc:cast(Peer, fun() ->
        case registry_lookup_vnode(VnodeId) of
            {ok, Pid} -> gen_server:cast(Pid, Message);
            _ -> ok
        end
    end).

registry_lookup_vnode(VnodeId) ->
    vordb_registry:lookup({kv_store, VnodeId}).

registry_start(_Name) ->
    vordb_registry:start().

%% ===== Vor Agent Start =====

start_kv_store(NodeId, VnodeId, SyncIntervalMs, Name) ->
    Opts = [{node_id, NodeId}, {vnode_id, VnodeId}, {sync_interval_ms, SyncIntervalMs}, {name, Name}],
    'Elixir.Vor.Agent.KvStore':start_link(Opts).

delete_by_prefix(Db, Iter, {ok, Key, _Value}, Prefix, PrefixLen) ->
    case Key of
        <<Prefix:PrefixLen/binary, _Rest/binary>> ->
            rocksdb:delete(Db, Key, []),
            delete_by_prefix(Db, Iter, rocksdb:iterator_move(Iter, next), Prefix, PrefixLen);
        _ ->
            delete_by_prefix(Db, Iter, rocksdb:iterator_move(Iter, next), Prefix, PrefixLen)
    end;
delete_by_prefix(_Db, _Iter, {error, invalid_iterator}, _Prefix, _PrefixLen) -> ok.

call_agent(Pid, Message) ->
    gen_server:call(Pid, Message).

cast_agent(Pid, Message) ->
    gen_server:cast(Pid, Message).

%% ===== Vor Message Constructors (for Gleam public API) =====

make_put_msg(Key, Value) -> {put, #{key => Key, value => Value}}.
make_get_msg(Key) -> {get, #{key => Key}}.
make_delete_msg(Key) -> {delete, #{key => Key}}.
make_set_add_msg(Key, Element) -> {set_add, #{key => Key, element => Element}}.
make_set_remove_msg(Key, Element) -> {set_remove, #{key => Key, element => Element}}.
make_set_members_msg(Key) -> {set_members, #{key => Key}}.
make_counter_increment_msg(Key, Amount) -> {counter_increment, #{key => Key, amount => Amount}}.
make_counter_decrement_msg(Key, Amount) -> {counter_decrement, #{key => Key, amount => Amount}}.
make_counter_value_msg(Key) -> {counter_value, #{key => Key}}.
make_get_stores_msg() -> {get_stores, #{}}.

%% ===== Full Sync =====

gossip_send_full_sync() ->
    case whereis(vordb_ring_manager) of
        undefined -> ok;
        _ ->
            Ring = vordb_ring_manager:get_ring(),
            SelfNode = vordb_ring_manager:self_node(),
            MyPartitions = vordb_ring_manager:my_partitions(),
            lists:foreach(fun(VnodeIndex) ->
                PrefList = 'vordb@ring':preference_list(Ring, VnodeIndex),
                Peers = [binary_to_atom(N) || N <- PrefList, N =/= SelfNode],
                case registry_lookup_vnode(VnodeIndex) of
                    {ok, Pid} ->
                        {stores, #{lww := Lww, sets := Sets, counters := Counters}} =
                            gen_server:call(Pid, {get_stores, #{}}),
                        lists:foreach(fun(Peer) ->
                            case maps:size(Lww) > 0 of
                                true -> erpc_cast_vnode(Peer, VnodeIndex, {lww_sync, #{remote_lww_store => Lww}});
                                false -> ok
                            end,
                            case maps:size(Sets) > 0 of
                                true -> erpc_cast_vnode(Peer, VnodeIndex, {set_sync, #{remote_set_store => Sets}});
                                false -> ok
                            end,
                            case maps:size(Counters) > 0 of
                                true -> erpc_cast_vnode(Peer, VnodeIndex, {counter_sync, #{remote_counter_store => Counters}});
                                false -> ok
                            end
                        end, Peers);
                    _ -> ok
                end
            end, MyPartitions)
    end.
