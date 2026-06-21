# Durbhash

Durbhash (दूरभाष) — Sanskrit for "telephone," literally "speaking from a distance."

A TCP chat server and client written in C, built from raw POSIX sockets and threads. Durbhash supports multiple concurrency models for handling client connections, implemented as a way to explore the tradeoffs between thread-per-connection, thread pooling, and I/O multiplexing.

## Features

- Real-time message broadcasting between connected clients
- Two concurrency modes: thread-per-connection and thread pool with `poll()`
- Unique username enforcement across all connected clients
- Graceful client disconnection (`quit` command) with server-confirmed handshake
- Dynamic, resizable client tracking in thread pool mode
- Thread-safe broadcast via mutex-protected shared client registries

## Architecture

### Default mode — thread-per-connection

Each accepted connection spawns a dedicated, detached thread. The thread blocks on `recv()` for that client exclusively, and a shared `client_fds[]` array (protected by a mutex) is used to broadcast incoming messages to every other connected client.

```
main thread
  └── accept() → fork a thread per client
        ├── thread 1 ← client A (blocks on recv)
        ├── thread 2 ← client B (blocks on recv)
        └── thread 3 ← client C (blocks on recv)
```

This is simple to reason about and works well at small scale, but does not scale to large numbers of concurrent clients due to per-thread memory overhead.

### Thread pool mode — `poll()` + producer-consumer

A fixed pool of worker threads is spawned at startup. The main thread's only job is to `accept()` new connections and dispatch them round-robin to a worker via a thread-safe bounded queue (producer-consumer pattern with a mutex and condition variable).

Each worker owns its own `poll()` event loop and its own private list of file descriptors — no locking is required within a worker, since only one thread ever touches its `poll_fds[]`. Broadcasting across workers (since a sender and recipient may be owned by different threads) is handled through a shared, mutex-protected client registry.

```
main thread (producer)
  └── accept() → queue_push(fd) → round-robin to next worker

worker 0 (consumer)            worker 1 (consumer)
  poll_fds: [A, B]               poll_fds: [C, D]
  queue_pop() for new clients    queue_pop() for new clients
  single poll() loop             single poll() loop

shared pool_clients[] registry (mutex-protected) used for cross-worker broadcast
```

This mode avoids the memory cost of one thread per client, and is closer to how production-grade servers handle large numbers of concurrent connections.

## Build

```bash
make
```

This compiles both `dbs-server` and `dbs-client` using the included `Makefile`.

## Run

Start the server in default (thread-per-connection) mode:

```bash
./dbs-server
```

Start the server in thread pool mode:

```bash
./dbs-server -p
# or
./dbs-server --thread-pool
```

Connect a client:

```bash
./dbs-client <hostname>
```

You can also connect using `nc` or `telnet` for quick testing:

```bash
nc localhost 50000
```

## Usage

Once connected, the server will prompt for a username:

```
Enter your name: Alice
Welcome, Alice!
```

Usernames must be unique — if a name is already taken, the server will prompt again. After registering, any message you type is broadcast to all other connected clients:

```
> Hello everyone!
```

To disconnect gracefully:

```
> quit
Goodbye!
```

## Design notes

A few implementation details worth highlighting for anyone reading the source:

- **Sentinel values over boolean flags.** Both `client_fds[]` and the pool mode registries use `-1` as a sentinel for "unused slot" rather than a separate active flag, since the mutex already guarantees atomicity of the check-and-set.
- **Swap-with-last removal.** When a client disconnects in pool mode, its slot is filled by swapping in the last active entry rather than leaving a gap — keeping the array dense and avoiding the need to skip inactive entries on every `poll()` call.
- **Heap-allocated thread arguments.** Any data passed into a new thread (file descriptors, names, client structs) is heap-allocated with `malloc`/`strdup` rather than passed by reference to a stack variable, avoiding a class of bugs where the parent's loop overwrites the argument before the child thread reads it.
- **Non-blocking broadcast.** All broadcast sends use `MSG_DONTWAIT` so that a slow or unresponsive client never stalls the thread sending to everyone else.
- **`SIGPIPE` handling.** The server ignores `SIGPIPE` so that sending to an already-disconnected client returns an error instead of terminating the process.

## Roadmap

Planned additions, in rough order of the historical evolution of real-time messaging architectures:

- [ ] Long polling mode
- [ ] WebSocket support
- [ ] Platform-specific event backend abstraction (`epoll` on Linux, `kqueue` on BSD/macOS, `poll()` fallback elsewhere)

## License

GPL v3.0
