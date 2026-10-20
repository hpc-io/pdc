# PHT create/add development contract

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

With `BUILD_TESTING=ON`, `pht_regression` exercises in-process server behavior,
binary input ownership, missing prefixes, repeated creation, capacity rejection,
and range endpoints. `pht_serialization` checks binary RPC encode/decode/free
using local shared-memory transport. `pht_create_bucket` and `pht_add_key` exercise
the client RPC paths through the usual test runner. AddressSanitizer and UndefinedBehaviorSanitizer
can be enabled through the compiler and linker flags for memory validation.
