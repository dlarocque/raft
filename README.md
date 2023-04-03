# Raft

Based off of MIT Distributed Systems Lab 2: https://pdos.csail.mit.edu/6.824/labs/lab-raft.html

## Run tests

In `internal/raft`,

```sh
go test
```

## Running Tests With Logging

If you have `internal/raft/dslogs` in your path,

```sh
VERBOSE=1 go test | dslogs -c <num_columns>
```

To run a specific test with logs,

```sh
VERBOSE=1 go test -run <test_name> | dslogs -c <num_columns>
```
