# Hinted Hash Templates

> Status: in development on branch `hinted-hash-dev` (feature starts at commit
> `f9036783d`). Target version: **8.10.0**. This document is both the feature
> reference and the PR description.
>
> PRD: *Efficient Import and Storage of Large Structured Datasets (aka HashV2)*.

## Summary

This PR introduces **Hinted Hash Templates (HHT)**. It brings two things.

### 1. A new command for faster hash ingestion: `HIMPORT`

Bulk-loading hashes with `HSET` re-sends the same field names on every key, and
each call pays repeated dict rehashing and command-argument allocation.
`HIMPORT` lets a client declare the field set **once** per connection, then
create keys by sending **values only**:

```
HIMPORT PREPARE user name email age          # declare the field set once
HIMPORT SET user:1 user alice a@example.com 30   # then stream values per key
HIMPORT SET user:2 user bob   b@example.com 25
```

This cuts both network traffic and ingestion CPU versus an `HSET key f1 v1
f2 v2 …` per key.

### 2. A new internal encoding for reduced memory: *templates*

When many hashes share the same layout (e.g. one hash per user, all with
`{name, email, age}`), the field names are pure per-key duplication. A
**template** is an internal, ordered field-name set that such keys *share*:
the names are stored **once**, and each key keeps only its values plus a small
reference to the template.

A template is an **opaque, internal encoding** — like `listpack` and
`hashtable`. Redis fully manages template creation, sharing, and destruction;
**users never name, see, or manage a template.** Hash semantics are unchanged
(`HGET`/`HSET`/`HDEL`/`HRANDFIELD`/HFE/…); the only user-visible signs are lower
memory and a new `OBJECT ENCODING` value (`template-listpack` /
`template-array`). Because it is hidden, Redis keeps full freedom to evolve it
(replication, upgrades, on-disk format) with no backward-compatibility contract.

### How the two connect

- **When `HIMPORT` is used**, Redis takes it as a hint that these keys share a
  layout and stores them template-backed — so `HIMPORT` delivers *both* faster
  ingestion *and* reduced memory in one path.
- **For existing workloads** that keep using `HSET`, a family of configs
  (`hash-min-template-entries`, see §2) can auto-encode large hashes as
  templates behind the scenes — so they gain the memory savings with **no code
  change**.

Both surfaces are optional and off by default.

---

## 1. The ingestion API (`HIMPORT`)

`HIMPORT` (container command, since 8.10.0) is the explicit, client-driven path.
The idea: send the field set **once** per connection, register it under a
session-local **fieldset name**, then refer to it by name on subsequent writes
sending **values only**.

```
HIMPORT PREPARE <fieldset_name> <field1> <field2> ...
HIMPORT SET     <key> <fieldset_name> <value1> <value2> ...
HIMPORT SET     <key> <fieldset_name> <value1> <value2> ...
...
HIMPORT DISCARD <fieldset_name>
```

A **fieldset** is purely a **connection-session hint**:

- It is *not* part of the dataset — never saved in RDB, never replicated, never
  visible to other clients. Another client wanting the same layout simply issues
  its own `HIMPORT PREPARE`.
- It is discarded automatically when the connection closes (and on `RESET`).
- It only *hints* at a field set; the actual template is interned internally on
  first use.

### `HIMPORT PREPARE <fieldset_name> <field…>`
Stores the ordered field-name list under `fieldset_name` for this connection
(overriding any previous one with that name).
Reply: `OK`. Error: `duplicate field name in fieldset`.

### `HIMPORT SET <key> <fieldset_name> <value…>`
Creates or overwrites `key` as a hash using the prepared field list paired
positionally with the values. On first use of a given field list, Redis interns
it as a template; later keys reuse it. Value count must equal the field count.
Reply: `OK`. Errors: `no such fieldset`,
`value count does not match fieldset field count`.

### `HIMPORT DISCARD <fieldset_name>`
Removes the fieldset from the connection. Does **not** affect templates already
created or keys already written. Reply: `1` if removed, `0` if absent.

### `HIMPORT DISCARDALL`
Removes all fieldsets from the connection. Reply: number removed.

---

## 2. Transparent auto-conversion (config)

For existing workloads that don't use `HIMPORT`, the
`hash-min-template-entries` family lets eligible hashes become template-backed
**behind the scenes**, with no application change — users keep using `HSET` etc.

| Config | Meaning | Default |
|---|---|---|
| `hash-min-template-entries` | Min field count at which a normal hash auto-converts to a template encoding (on write and on RDB load). `0` = disabled. | `0` |
| `hash-max-template-entries` | Upper bound of the auto-convert field-count window. | `0` |
| `hash-rdb-load-min-template-entries` | RDB-load-only lower bound (load compactly even if the live threshold differs). | `0` |
| `hash-rdb-load-max-template-entries` | RDB-load-only upper bound. | `0` |
| `hash-rdb-load-template-disassembly-threshold` | After load, templates referenced by fewer than this many keys are disassembled back to plain hashes (not worth the shared-template overhead for one or two keys). | `0` |

All are `MODIFIABLE_CONFIG` and applied **lazily** (like
`hash-max-listpack-entries`): an existing hash is only re-encoded on its next
write, not immediately on `CONFIG SET`.

Conversion is one-directional w.r.t. the threshold: a hash converts **up** to a
template when it crosses the min; shrinking does not auto-revert (see §8).

> Dev-only / **remove before merge:** `hash-template-mask-encoding` (when `yes`,
> `OBJECT ENCODING` reports the plain equivalent so the legacy hash test-suite
> passes with auto-convert on).

---

# Internals (maintainers / agents)

Everything below is implementation detail — not a user contract.

## 3. Templates

In the code a template is a `hashTemplate`. Properties:

- An **immutable**, **sorted** list of field names with a small runtime **id**.
- **Shared** across keys and **reference-counted**; deleted when the refcount
  hits zero. Creation/destruction is implicit and fully internal.
- **Deduplicated**: two keys with the same field-name set (in any order) share
  one template.

Field lookup within a key is a binary search over the sorted field array
(`hashTemplateFieldIndex`), which is why field lists are kept sorted and unique
(`hashTemplateValidateFields`).

## 4. Encodings

Two new object encodings (`src/object.h`), opaque like `listpack`/`hashtable`:

| Encoding | id | `o->ptr` layout | Used when |
|---|---|---|---|
| `OBJ_ENCODING_TMPL_LP` | 14 | listpack `[template_id (varint)][value0]…[valueN-1]` | values fit listpack limits |
| `OBJ_ENCODING_TMPL_ARRAY` | 15 | `hashTemplateArray { hashTemplate *tmpl; sds values[]; }` | a value/field-count exceeds listpack limits |

Field names live only in the template, never in the object. `TMPL_LP` is the
compact form (id is a 1–2 byte varint); `TMPL_ARRAY` embeds the template pointer
directly. Listpack thresholds are the usual `hash-max-listpack-entries` /
`hash-max-listpack-value`.

## 5. The registry

`hashTemplates` (`server.htemplates`, `src/server.h`) holds three indexes onto
the same templates:

- `registry` — keyed by a **commutative** field-set hash (Σ per-field siphash),
  so adding/removing one field updates the bucket key in O(1) for the HSET/HDEL
  paths (full field list still compared on match).
- `by_id` — dense small-int id → template, for `TMPL_LP` (which stores the id).
- `by_fields_lp` — serialized field-name blob → template, so a self-contained
  `RESTORE` resolves its template in one O(1) lookup.

## 6. Reference counting & lifetime

Two counts keep a template alive:

- **`key_refcount`** (atomic) — number of live keys. Atomic because a BIO
  lazyfree thread may free a key off the main thread.
- **`hold_refcount`** — non-key holders: a connection's `HIMPORT PREPARE`
  fieldset, an in-progress RDB load, the `hsetc_cache`.

When both reach zero the template is reclaimed. A BIO thread that drops the last
key can't touch the registry, so it enqueues the template **id** into
`pending_free_ids` under `htemplates->lock`; the main thread drains it in
`hashTemplateDrainPendingFree` (from `serverCron`), re-checks both counts, and
frees. `htemplates->lock` guards only `pending_free_ids` + `by_id`; the registry,
`hold_refcount`, and propagation argv are main-thread-only. (This is the most
safety-critical path — it had a concurrency UAF fixed during development.)

## 7. Encoding transitions

`hashTypeConvert(db, o, enc)` handles all conversions:

- **plain → template:** `hashTypeTryConvertToTemplate()` when enabled (config or
  HIMPORT) — `TMPL_LP` if values fit a listpack, else `TMPL_ARRAY`.
- **`TMPL_LP` → `TMPL_ARRAY`:** when a value crosses `hash-max-listpack-value`,
  the field count crosses `hash-max-listpack-entries`, or the template can't use a
  listpack.
- **template → plain:** RDB-load disassembly of few-key templates (§9).

## 8. Mutating a template-backed hash

Templates are immutable, so `HSET` of a **new** field or `HDEL` **detaches** the
key from its current template and attaches it to a (new or existing) template for
the updated field set — **exact-match** only. The commutative field-hash makes
finding/creating that neighbor template cheap. Consequences:

- Once a key is template-backed it stays template-backed after mutation (it just
  moves between templates); `HDEL` of the last field deletes the key.
- This is deterministic, so replicas reproduce it identically.
- A key is attached only to an **exact-match** template; there is no partial /
  best-fit matching and no "leave removed fields empty" representation.

## 9. Persistence

Layouts are deterministic across restart / replication / migration: a key's
reduced footprint is preserved everywhere.

**RDB** (`src/rdb.h`) stores the registry once at the top via opcode
`RDB_OPCODE_HASH_TEMPLATES` (242), then keys in compact **ref** form. Two
variants per encoding:

| Type | id | Form | Used by |
|---|---|---|---|
| `RDB_TYPE_HASH_TMPL_LP` | 29 | self-contained: `[count][f0]…[fN-1][lp_blob]` | DUMP |
| `RDB_TYPE_HASH_TMPL_LP_REF` | 30 | ref: raw lp blob (first entry = id) | RDB save |
| `RDB_TYPE_HASH_TMPL_ARRAY` | 31 | self-contained: `[count][f0][v0]…` | DUMP |
| `RDB_TYPE_HASH_TMPL_ARRAY_REF` | 32 | ref: `[id][v0]…[vN-1]` | RDB save |

On load, `rdbLoadHashTemplates` rebuilds the registry (each template hold-ref'd);
`rdbClearHashTemplates` releases those refs and is wired into
`rdbLoadRioWithLoadingCtx` so it covers **every** load path (disk startup,
diskless replica, AOF rdb-preamble) — missing this caused "Duplicate hash
template ID" crashes on the second resync/loadaof, fixed during dev. RDB load
can also template/disassemble using the `hash-rdb-load-*` knobs (§2), tracked by
a per-load `rdbLoadTemplateCtx`.

**DUMP / RESTORE** uses the **self-contained** types (29/31): field names are
inlined so the payload is portable, but it carries an indication that the loader
should rebuild a *template-backed* hash (preserving the memory optimization).

**AOF** rewrite emits the keys so replay reconstructs the template encoding.

## 10. Replication

Replication is unchanged except for **one internal command**. `HIMPORT SET`
propagates to replicas/AOF as an internal, **opaque** `HSETC` command — never
issued by users, flagged `INTERNAL | NOSCRIPT`. It tells the consumer the key is
eligible for template-based compaction so the replica/migration destination
rebuilds the same encoding. `HSETC key f0…fN-1 v0…vN-1` carries the full field
list plus values; a per-client `hsetc_cache` skips the registry lookup on
repeated same-template applies. It is an implementation detail of propagation,
not a user-facing command, and is intentionally not documented as API.

## 11. Cluster / slot migration

Template-backed keys migrate via the self-contained DUMP/RESTORE form (field names
inlined) and are rebuilt with a fresh local template id on the destination, which
is a live node with its own id namespace.

## 12. Introspection & memory accounting

- **`INFO stats`:** `hash_templates` (distinct templates), `hash_template_keys`
  (Σ `key_refcount`, O(1) via an atomic counter).
- **`INFO memory`:** `used_memory_hash_templates` — bytes held by the registry,
  added to `overhead.total` like `used_memory_functions`.
- **`MEMORY STATS`:** `hash.templates` — same value.

The memory figure is an **O(1) incremental counter** (`total_mem_size`, `±=`
each template's `mem_size` on create/free) rather than a registry walk, because
there may be ~100k templates and `INFO` is polled often. The shared template is
**not** attributed to any key: `MEMORY USAGE <key>` reports a key's marginal
cost (values + id reference), and the shared template is counted once server-wide
in the figures above.

## 13. Concurrency summary

`key_refcount` is the only off-main-thread field (atomic); its zero-transition
only enqueues an id under the lock. `htemplates->lock` guards `pending_free_ids`
+ `by_id`. Registry mutation, `hold_refcount`, propagation argv, and template
revival (0→1) are main-thread-only. A `TMPL_LP` free may run on a BIO thread
(`hashTemplateLpFree`, resolving id → template under the lock).

## 14. Tests

`tests/unit/type/hash-templates.tcl` (multiple `start_server` blocks for
the different config regimes) covers both encodings end-to-end, large field
counts across the stack/heap boundary, HRANDFIELD, DUMP/RESTORE, repeated full
resyncs and AOF rdb-preamble loads, the BIO-free vs main-thread-drop lifecycle
fuzzer, active defrag, RDB-load disassembly + alloc-size histogram consistency,
and INFO/MEMORY STATS reporting. Integration coverage in
`tests/integration/replication.tcl` and `tests/unit/dump.tcl`.

## 15. Files changed (PR overview)

- `src/t_hash.c` — registry, templates, refcounting, encodings, conversions,
  `HIMPORT`/`HSETC`, RDB-load guard, HRANDFIELD, memory accounting (bulk of it).
- `src/server.h` — template/registry/array structs, `redisMemOverhead`, prototypes.
- `src/object.h` / `src/object.c` — encodings, encoding reporting, memory wiring.
- `src/rdb.c` / `src/rdb.h` — RDB types, registry opcode, save/load,
  DUMP/RESTORE, load-time disassembly.
- `src/aof.c` — AOF rewrite.
- `src/config.c` — the auto-conversion configs.
- `src/db.c` — per-slot allocation-size histogram interaction.
- `src/commands/hsetc.json`, `src/commands/himport*.json` (+ `commands.def`).
- tests as above.

## 16. Temporary (remove before merge)

- `hash-template-mask-encoding` config — exists only so the legacy hash
  test-suite passes with auto-conversion on (it makes `OBJECT ENCODING` /
  `DEBUG OBJECT` report the plain equivalent).
- `allocateTemplateId` uses a linear smallest-free-id scan (marked
  `TODO: faster way?`).
