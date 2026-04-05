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

%% ===== Storage Lifecycle =====

storage_start(DataDir) ->
    filelib:ensure_dir(filename:join(DataDir, "dummy")),
    gen_server:start_link({local, vordb_storage}, ?MODULE, DataDir, []).

storage_stop() ->
    gen_server:stop(vordb_storage).

%% ===== Gen_server callbacks =====

init(DataDir) ->
    DbOpts = [{create_if_missing, true}, {write_buffer_size, 64 * 1024 * 1024}, {max_open_files, 1000}],
    DirStr = case is_binary(DataDir) of
        true -> binary_to_list(DataDir);
        false -> DataDir
    end,
    case rocksdb:open(DirStr, DbOpts) of
        {ok, Db} -> {ok, #{db => Db}};
        {error, Reason} -> {stop, {rocksdb_open_failed, Reason}}
    end.

handle_call({put, Key, Value}, _From, #{db := Db} = State) ->
    Encoded = erlang:term_to_binary(Value),
    case rocksdb:put(Db, Key, Encoded, []) of
        ok -> {reply, ok, State};
        {error, R} -> {reply, {error, R}, State}
    end;

handle_call({get, Key}, _From, #{db := Db} = State) ->
    case rocksdb:get(Db, Key, []) of
        {ok, Bin} -> {reply, {ok, erlang:binary_to_term(Bin)}, State};
        not_found -> {reply, not_found, State};
        {error, R} -> {reply, {error, R}, State}
    end;

handle_call({delete, Key}, _From, #{db := Db} = State) ->
    case rocksdb:delete(Db, Key, []) of
        ok -> {reply, ok, State};
        {error, R} -> {reply, {error, R}, State}
    end;

handle_call({get_all_prefix, Prefix}, _From, #{db := Db} = State) ->
    {ok, Iter} = rocksdb:iterator(Db, []),
    Entries = iterate_prefix(Iter, rocksdb:iterator_move(Iter, first), Prefix, byte_size(Prefix), #{}),
    rocksdb:iterator_close(Iter),
    {reply, Entries, State};

handle_call({put_all_prefix, Prefix, Entries}, _From, #{db := Db} = State) ->
    Batch = maps:fold(fun(K, V, Acc) ->
        [{put, <<Prefix/binary, K/binary>>, erlang:term_to_binary(V)} | Acc]
    end, [], Entries),
    case rocksdb:write(Db, Batch, []) of
        ok -> {reply, ok, State};
        {error, R} -> {reply, {error, R}, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #{db := Db}) ->
    rocksdb:close(Db).

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

pad(VnodeId) ->
    iolist_to_binary(io_lib:format("~2..0B", [VnodeId])).

make_prefix(Type, VnodeId) ->
    <<Type/binary, ":", (pad(VnodeId))/binary, ":">>.

make_key(Type, VnodeId, Key) ->
    <<(make_prefix(Type, VnodeId))/binary, Key/binary>>.

storage_put_lww(VnodeId, Key, Value) ->
    gen_server:call(vordb_storage, {put, make_key(<<"lww">>, VnodeId, Key), Value}).
storage_put_set(VnodeId, Key, Value) ->
    gen_server:call(vordb_storage, {put, make_key(<<"set">>, VnodeId, Key), Value}).
storage_put_counter(VnodeId, Key, Value) ->
    gen_server:call(vordb_storage, {put, make_key(<<"counter">>, VnodeId, Key), Value}).

storage_get_all_lww(VnodeId) ->
    gen_server:call(vordb_storage, {get_all_prefix, make_prefix(<<"lww">>, VnodeId)}).
storage_get_all_sets(VnodeId) ->
    gen_server:call(vordb_storage, {get_all_prefix, make_prefix(<<"set">>, VnodeId)}).
storage_get_all_counters(VnodeId) ->
    gen_server:call(vordb_storage, {get_all_prefix, make_prefix(<<"counter">>, VnodeId)}).

storage_put_all_lww(VnodeId, Entries) ->
    gen_server:call(vordb_storage, {put_all_prefix, make_prefix(<<"lww">>, VnodeId), Entries}).
storage_put_all_sets(VnodeId, Entries) ->
    gen_server:call(vordb_storage, {put_all_prefix, make_prefix(<<"set">>, VnodeId), Entries}).
storage_put_all_counters(VnodeId, Entries) ->
    gen_server:call(vordb_storage, {put_all_prefix, make_prefix(<<"counter">>, VnodeId), Entries}).

%% ===== Entry helpers =====

entry_new(Value, Timestamp, NodeId) ->
    #{value => Value, timestamp => Timestamp, node_id => NodeId}.

entry_tombstone(Timestamp, NodeId) ->
    #{value => '__tombstone__', timestamp => Timestamp, node_id => NodeId}.

entry_lookup(Store, Key) ->
    case maps:get(Key, Store, not_found) of
        not_found -> #{val => none, found => false};
        #{value := '__tombstone__'} -> #{val => none, found => false};
        #{value := V} -> #{val => V, found => true}
    end.

%% ===== OR-Set helpers =====

orset_empty() -> #{entries => #{}, tombstones => #{}}.

orset_get_or_empty(Store, Key) ->
    maps:get(Key, Store, orset_empty()).

orset_has_key(Store, Key) ->
    maps:is_key(Key, Store).

orset_add_element(SetState, Element, Tag) ->
    Entries = maps:get(entries, SetState),
    Existing = maps:get(Element, Entries, #{}),
    NewTags = maps:put(Tag, true, Existing),
    SetState#{entries := maps:put(Element, NewTags, Entries)}.

orset_remove_element(SetState, Element) ->
    Entries = maps:get(entries, SetState),
    Tombstones = maps:get(tombstones, SetState),
    case maps:get(Element, Entries, undefined) of
        undefined -> SetState;
        Tags ->
            ExistingTombs = maps:get(Element, Tombstones, #{}),
            NewTombs = maps:merge(ExistingTombs, Tags),
            SetState#{tombstones := maps:put(Element, NewTombs, Tombstones)}
    end.

orset_read_elements(SetState) ->
    Entries = maps:get(entries, SetState),
    Tombstones = maps:get(tombstones, SetState),
    Elements = maps:fold(fun(Element, Tags, Acc) ->
        DeadTags = maps:get(Element, Tombstones, #{}),
        LiveTags = maps:without(maps:keys(DeadTags), Tags),
        case maps:size(LiveTags) > 0 of
            true -> [Element | Acc];
            false -> Acc
        end
    end, [], Entries),
    lists:sort(Elements).

orset_make_tag(NodeId, Timestamp, Counter) ->
    #{node_id => NodeId, timestamp => Timestamp, counter => Counter}.

orset_merge_stores(Local, Remote) ->
    maps:merge_with(fun(_K, L, R) -> orset_merge(L, R) end, Local, Remote).

orset_merge(Local, Remote) ->
    #{
        entries => merge_tag_maps(maps:get(entries, Local), maps:get(entries, Remote)),
        tombstones => merge_tag_maps(maps:get(tombstones, Local), maps:get(tombstones, Remote))
    }.

merge_tag_maps(A, B) ->
    maps:merge_with(fun(_K, TagsA, TagsB) -> maps:merge(TagsA, TagsB) end, A, B).

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
    Peers = nodes(),
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
    Peers = nodes(),
    NumVnodes = application:get_env(vordb, num_vnodes, 16),
    case Peers of
        [] -> ok;
        _ ->
            lists:foreach(fun(VnodeIndex) ->
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
            end, lists:seq(0, NumVnodes - 1))
    end.
