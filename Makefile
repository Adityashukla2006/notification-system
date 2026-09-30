# Benchmarks. Every target runs bench/run.sh against an isolated Docker stack
# (compose project "notibench"), never the development database.
#
#   make bench            the full suite: up, env, every test, report
#   make bench-<test>     one test: pilot enqueue ingest scaling e2e failures crash
#                         idempotency fairness burst
#   make bench-report     rebuild bench/results/summary.md from raw outputs
#   make bench-down       delete the stack and its volume
#
# Test parameters are environment variables read by bench/run.sh, e.g.
#   make bench-e2e E2E_RATES="300 900"   REPS=3

SHELL := bash
BENCH := bash bench/run.sh
TESTS := pilot enqueue ingest scaling e2e failures crash idempotency fairness burst

.PHONY: bench bench-up bench-env bench-down bench-report $(addprefix bench-,$(TESTS))

bench: bench-up bench-env $(addprefix bench-,$(TESTS)) bench-report

bench-up:
	$(BENCH) up

bench-env:
	$(BENCH) env

bench-down:
	$(BENCH) down

bench-report:
	python bench/summarize.py

$(addprefix bench-,$(TESTS)): bench-%:
	$(BENCH) $*
