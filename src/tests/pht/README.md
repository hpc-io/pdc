# PHT create/add development contract

PHT is experimental and disabled by default. Enable it with
`-DPDC_ENABLE_PHT=ON` when configuring CMake. The default build excludes its
implementation, client APIs, RPC registration, server state, and PHT tests.
Standard object/container tags and queries continue to use the SOMETA DHT path
with either setting; enabling PHT exposes separate development operations.

Bucket creation is idempotent. Prefix lookup reports a missing bucket without
accessing it. Key insertion copies exactly `size` bytes, including embedded NUL
bytes; repeated equal byte sequences are deduplicated. A successful response has
`ret == 1`; rejected operations return `FAIL` and set `ret == -1`.

The current implementation supports up to 1024 distinct keys per bucket. Further
values for an existing key are allowed. New keys beyond the limit fail without
changing the bucket or its stored values. Distributed splitting and redistribution
are not implemented. Metadata deletion also returns `FAIL` until its protocol is
implemented. These development APIs do not yet provide object associations,
retrieval, or persistence across server restarts.

With `BUILD_TESTING=ON` and `PDC_ENABLE_PHT=ON`, `pht_regression` exercises in-process server behavior,
binary input ownership, missing prefixes, repeated creation, capacity rejection,
and range endpoints. `pht_serialization` checks binary RPC encode/decode/free
using the transport selected by `HG_TRANSPORT` (default `ofi+tcp`). `pht_create_bucket` and `pht_add_key` exercise
the client RPC paths through the usual test runner. AddressSanitizer and UndefinedBehaviorSanitizer
can be enabled through the compiler and linker flags for memory validation.

The always-built `someta_metadata` regression checks container tags, binary
object-tag payloads, exact query result IDs across the DHT, empty queries, and tag replacement.
It runs with PHT disabled or enabled. For distributed runs, pass the number of
PDC servers to `someta_metadata` and launch one or more MPI clients after the
servers have written their connection file.

An optional second argument selects an even object count per client (default 8).
For example, `someta_metadata 1 8192` validates 4096 returned IDs (32 KiB)
before replacement, exercising responses larger than the usual eager message
buffer. The server should close without Mercury bulk-handle warnings after both
small and large queries. Query results are copied into Mercury-owned response
storage, while retaining the existing serialized bulk-handle format.

`someta_serialization` checks empty, small, and oversized responses with the
legacy decoder after the original result storage has been destroyed. It also
checks serialization with shared-memory processor flags.
