.PHONY: build test vor proto clean verify verify-check verify-chaos combined

VOR_DIR := ../vor
VOR_EBIN := $(VOR_DIR)/_build/dev/lib/vor/ebin
VOR_DEPS_EBIN := $(VOR_DIR)/_build/dev/lib/nimble_parsec/ebin
VORDB_EBIN := build/dev/erlang/vordb/ebin
VORDB_ROOT := $(CURDIR)
COMBINED := $(CURDIR)/build/verify/kv_combined.vor

build: proto vor
	gleam build

proto:
	@if [ ! -f src/vordb_pb.erl ] || [ proto/vordb.proto -nt src/vordb_pb.erl ]; then \
		gleam build 2>/dev/null; \
		erl -pa build/dev/erlang/*/ebin -noshell -eval 'ok = gpb_compile:file("proto/vordb.proto", [{i, "proto"}, {o_erl, "src"}, {o_hrl, "src"}, maps, {maps_unset_optional, omitted}, strings_as_binaries, {module_name, "vordb_pb"}]), io:format("Proto compiled~n"), halt().'; \
	fi

vor:
	cd $(VOR_DIR) && mix compile --no-deps-check
	mkdir -p $(VORDB_EBIN)
	elixir -pa $(VOR_EBIN) -pa $(VOR_DEPS_EBIN) -e 'source = File.read!("src/vor/kv_store.vor"); {:ok, result} = Vor.Compiler.compile_string(source); beam_path = "$(VORDB_EBIN)/#{result.module}.beam"; File.write!(beam_path, result.binary); IO.puts("Compiled kv_store.vor → #{result.module}")'

test: proto vor
	gleam build
	cp $(VORDB_EBIN)/Elixir.Vor.Agent.KvStore.beam $(VORDB_EBIN)/../../../test/erlang/vordb/ebin/ 2>/dev/null || true
	gleam test

# ---------------------------------------------------------------------------
# Verification. Every figure quoted in README.md / docs/PROJECT_OVERVIEW.md is
# produced by these targets. See verify/README.md for what they actually cover
# (and what they do not).
# ---------------------------------------------------------------------------

verify: verify-check verify-chaos

# Vor's system block needs the agent definition in the same source file.
combined:
	@mkdir -p $(dir $(COMBINED))
	@cat src/vor/kv_store.vor src/vor/kv_cluster.vor > $(COMBINED)

verify-check: combined
	cd $(VOR_DIR) && mix vor.check --deep $(COMBINED)
	@echo "--- raw stats (relevance, coverage, state count) ---"
	cd $(VOR_DIR) && mix run $(VORDB_ROOT)/verify/check_stats.exs $(COMBINED)

# Full-stack: puts VorDB's beams on the code path and starts RocksDB + ETS
# before the simulator, so the agents run real code. Plain `mix vor.simulate`
# does not, and every store ends up holding an extern-error tuple (F-006).
verify-chaos: build combined
	cd $(VOR_DIR) && mix run $(VORDB_ROOT)/verify/chaos.exs $(COMBINED) $(VORDB_ROOT) 42 30000
	cd $(VOR_DIR) && mix run $(VORDB_ROOT)/verify/chaos.exs $(COMBINED) $(VORDB_ROOT) 123 30000 --partition --delay
	cd $(VOR_DIR) && mix run $(VORDB_ROOT)/verify/chaos.exs $(COMBINED) $(VORDB_ROOT) 777 60000 --partition --delay

clean:
	gleam clean
	rm -f $(VORDB_EBIN)/Elixir.Vor.Agent.KvStore.beam
	rm -f src/vordb_pb.erl
	rm -rf build/verify
