.PHONY: build test vor proto clean

VOR_DIR := ../vor
VOR_EBIN := $(VOR_DIR)/_build/dev/lib/vor/ebin
VOR_DEPS_EBIN := $(VOR_DIR)/_build/dev/lib/nimble_parsec/ebin
VORDB_EBIN := build/dev/erlang/vordb/ebin

build: proto vor
	gleam build

proto:
	@if [ ! -f src/vordb_pb.erl ] || [ proto/vordb.proto -nt src/vordb_pb.erl ]; then \
		gleam build 2>/dev/null; \
		erl -pa build/dev/erlang/*/ebin -noshell -eval 'ok = gpb_compile:file("proto/vordb.proto", [{i, "proto"}, {o_erl, "src"}, {o_hrl, "src"}, maps, {maps_unset_optional, omitted}, {module_name, "vordb_pb"}]), io:format("Proto compiled~n"), halt().'; \
	fi

vor:
	cd $(VOR_DIR) && mix compile --no-deps-check
	mkdir -p $(VORDB_EBIN)
	elixir -pa $(VOR_EBIN) -pa $(VOR_DEPS_EBIN) -e 'source = File.read!("src/vor/kv_store.vor"); {:ok, result} = Vor.Compiler.compile_string(source); beam_path = "$(VORDB_EBIN)/#{result.module}.beam"; File.write!(beam_path, result.binary); IO.puts("Compiled kv_store.vor → #{result.module}")'

test: proto vor
	gleam build
	cp $(VORDB_EBIN)/Elixir.Vor.Agent.KvStore.beam $(VORDB_EBIN)/../../../test/erlang/vordb/ebin/ 2>/dev/null || true
	gleam test

clean:
	gleam clean
	rm -f $(VORDB_EBIN)/Elixir.Vor.Agent.KvStore.beam
	rm -f src/vordb_pb.erl
