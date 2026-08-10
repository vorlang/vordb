file = System.argv() |> List.first()
source = File.read!(file)

opts = [
  max_depth: 50,
  max_states: 200_000,
  integer_bound: 3,
  max_queue: 4,
  allow_vacuous: true,
  fire_timers: true,
  por: true
]

case Vor.Explorer.check_file(source, opts) do
  {:ok, verdict, stats} ->
    IO.puts("VERDICT: #{inspect(verdict)}")
    IO.puts("states_explored: #{stats.states_explored}")
    IO.puts("max_depth_reached: #{stats.max_depth_reached}")
    IO.puts("\n--- VACUITY ---")
    IO.inspect(stats.vacuity, limit: :infinity, printable_limit: :infinity)
    IO.puts("\n--- COVERAGE ---")
    IO.inspect(Map.get(stats, :coverage), limit: :infinity, printable_limit: :infinity)
    IO.puts("\n--- RELEVANCE ---")
    IO.inspect(Map.get(stats, :relevance), limit: :infinity, printable_limit: :infinity)
    IO.puts("\n--- LIVENESS ---")
    IO.inspect(Map.get(stats, :liveness), limit: :infinity, printable_limit: :infinity)

  other ->
    IO.puts("NON-OK RESULT:")
    IO.inspect(other, limit: :infinity, printable_limit: :infinity)
end
