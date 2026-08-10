# Full-stack chaos run: put VorDB's compiled beams on the code path and start
# the storage/cache/dirty-tracker infrastructure the KvStore agent's externs
# need, then run the Vor simulator against the VorDB system block.
#
#   mix run fullstack_sim.exs <combined.vor> <vordb_root> <seed> <duration_ms> [--partition] [--delay]

[file, vordb_root, seed, duration | flags] = System.argv()
seed = String.to_integer(seed)
duration = String.to_integer(duration)

Path.wildcard(Path.join(vordb_root, "build/dev/erlang/*/ebin"))
|> Enum.each(&Code.prepend_path/1)

data_dir = Path.join(System.tmp_dir!(), "vordb_chaos_#{seed}_#{duration}")
File.rm_rf!(data_dir)
File.mkdir_p!(data_dir)

# Infrastructure the KvStore externs call into.
{:ok, _} = :vordb_ffi.storage_start(data_dir)
:vordb_cache.init()
:vordb_dirty_tracker.init()

config = %{
  duration_ms: duration,
  seed: seed,
  kill_interval: {3_000, 10_000},
  fault_interval: {3_000, 10_000},
  check_interval_ms: 1_000,
  verbose: false,
  inject_faults: true,
  enable_partitions: "--partition" in flags,
  enable_delays: "--delay" in flags,
  delay_range: 50..200,
  partition_duration: {1_000, 5_000},
  workload_rate: 10
}

IO.puts("== full-stack chaos: seed #{seed}, #{duration}ms, partitions=#{config.enable_partitions}, delays=#{config.enable_delays}")

result = Vor.Simulator.run_file(file, config)

IO.puts("\n===== RESULT =====")

case result do
  {:ok, verdict, stats} ->
    IO.puts("verdict: #{verdict}")
    IO.puts("checks: #{stats.invariant_checks}  faults: #{stats.faults_injected}")
    IO.puts("workload: #{Map.get(stats, :workload_sent, 0)} sent / #{Map.get(stats, :workload_ok, 0)} ok / #{Map.get(stats, :workload_errors, 0)} err / #{Map.get(stats, :workload_timeouts, 0)} timeout")
    IO.puts("\n-- integrity --")
    IO.inspect(Map.get(stats, :integrity), limit: :infinity)
    IO.puts("\n-- relevance --")
    IO.inspect(Map.get(stats, :relevance), limit: :infinity)
    IO.puts("\n-- coverage --")
    IO.inspect(Map.get(stats, :coverage), limit: :infinity, printable_limit: :infinity)
    IO.puts("\n-- monitored --")
    IO.inspect(Map.get(stats, :monitored), limit: :infinity)

  other ->
    IO.inspect(other, limit: :infinity, printable_limit: :infinity)
end

# Show the live agent state at the end — did the stores actually hold data?
for name <- [:v1, :v2, :v3] do
  case Process.whereis(name) do
    nil -> IO.puts("#{name}: not alive")
    pid ->
      st = :sys.get_state(pid)
      IO.puts("#{name}: lww=#{inspect(Map.get(st, :lww_store), limit: 3)} sets=#{inspect(Map.get(st, :set_store), limit: 3)} counters=#{inspect(Map.get(st, :counter_store), limit: 3)}")
  end
end
