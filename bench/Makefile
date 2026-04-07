.PHONY: build run run-latency run-throughput clean

VORDB_EBIN := ../build/dev/erlang/vordb/ebin
GLEAM_DEPS := ../build/dev/erlang/*/ebin
BENCH_EBIN := ebin

build:
	@mkdir -p $(BENCH_EBIN)
	@cd .. && gleam build >/dev/null 2>&1 || true
	erlc -pa $(VORDB_EBIN) -o $(BENCH_EBIN) \
		src/vordb_bench_node.erl \
		src/vordb_bench.erl \
		src/vordb_bench_cluster.erl

run: run-all

run-all: build
	erl -pa $(GLEAM_DEPS) -pa $(BENCH_EBIN) -noshell -s vordb_bench main all

run-latency: build
	erl -pa $(GLEAM_DEPS) -pa $(BENCH_EBIN) -noshell -s vordb_bench main scenario1

run-throughput: build
	erl -pa $(GLEAM_DEPS) -pa $(BENCH_EBIN) -noshell -s vordb_bench main scenario2

run-eprof: build
	erl -pa $(GLEAM_DEPS) -pa $(BENCH_EBIN) -noshell -s vordb_bench main eprof

run-crdt: build
	erl -pa $(GLEAM_DEPS) -pa $(BENCH_EBIN) -noshell -s vordb_bench main scenario5

run-balance: build
	erl -pa $(GLEAM_DEPS) -pa $(BENCH_EBIN) -noshell -s vordb_bench main scenario9

run-convergence: build
	erl -sname bench_parent -pa $(GLEAM_DEPS) -pa $(BENCH_EBIN) -noshell -s vordb_bench main scenario7

clean:
	rm -rf $(BENCH_EBIN)
