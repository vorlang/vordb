.PHONY: build test vor clean

VOR_DIR := ../vor
VOR_EBIN := $(VOR_DIR)/_build/dev/lib/vor/ebin
VOR_DEPS_EBIN := $(VOR_DIR)/_build/dev/lib/nimble_parsec/ebin
VORDB_EBIN := build/dev/erlang/vordb/ebin

build: vor
	gleam build

vor:
	cd $(VOR_DIR) && mix compile --no-deps-check
	mkdir -p $(VORDB_EBIN)
	elixir -pa $(VOR_EBIN) -pa $(VOR_DEPS_EBIN) -e 'source = File.read!("src/vor/kv_store.vor"); {:ok, result} = Vor.Compiler.compile_string(source); beam_path = "$(VORDB_EBIN)/#{result.module}.beam"; File.write!(beam_path, result.binary); IO.puts("Compiled kv_store.vor → #{result.module}")'

test: vor
	gleam build
	cp $(VORDB_EBIN)/Elixir.Vor.Agent.KvStore.beam $(VORDB_EBIN)/../../../test/erlang/vordb/ebin/ 2>/dev/null || true
	gleam test

clean:
	gleam clean
	rm -f $(VORDB_EBIN)/Elixir.Vor.Agent.KvStore.beam
