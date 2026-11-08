# Paxos-replicated key-value store

A Go implementation of multi-instance Paxos and a replicated in-memory key-value service, developed for UCLA CS 134. The service orders `Get`, `Put`, and `Append` operations through consensus and deduplicates retried requests.

## Design

- `paxos/`: concurrent consensus instances, prepare/accept/decide RPCs, proposal numbering, and reclamation of instances after peers report completion.
- `kvpaxos/`: replicated operations, ordered application of decided log entries, request identifiers, cached responses, and retrying clients.
- Tests exercise competing proposers, unreliable communication, minority/majority partitions, reconnection, concurrent appends, missing log entries, and state reclamation.

RPCs use Go's `net/rpc` over Unix-domain sockets. These tests run multiple peers locally; this is an educational implementation, not a deployed storage service.

## Run

Requires Go 1.24.2 or newer on macOS or Linux.

```sh
go test ./paxos ./kvpaxos -timeout 180s
```

Both supplied package test suites passed locally on November 8, 2026. Tests include randomized fault scenarios; a passing run is not a formal proof of linearizability.

## Scope and limitations

State is held in memory. This repository does not implement durable recovery after a process restart, dynamic membership, or sharding. The service needs a communicating majority to make progress. A single client object is intended for sequential use; tests use separate clients for concurrent workloads.

## Coursework provenance

Extracted from the CS 134 key-value-store assignment workspace (`cs134-25s-kv`). The course supplied scaffolding and tests; the Paxos and replicated-service implementation is the project work. The original starter README is retained as `COURSE_STARTER_README.md` for context. Unrelated primary/backup and unfinished sharding packages are excluded. No new license is asserted over course-provided material.
