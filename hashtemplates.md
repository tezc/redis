# Hinted Hash Templates

> Status: in development on branch `hinted-hash-dev` (feature starts at commit
> `f9036783d`). Target version: **8.10.0**. This document is both the feature
> reference and the PR description.

## Summary

This PR introduces **Hinted Hash Templates (HHT)**, a way to cut the memory
used by hashes that share the same field names.

When many hashes have the same layout (for example one hash per user, each with
`name`, `email`, `age`), every key normally stores its own copy of those field
names. HHT stores the shared field-name set **once**, in an internal *template*;
each key then keeps only its values plus a small reference to it. Redis creates,
shares (reference-counted), and frees templates automatically.

This is an **internal encoding**: for most use cases users won't tell the
difference. Existing hash commands keep working exactly as before, with the same
semantics and replies; the hash just uses less memory. The application never
creates or sees a template.

The PR exposes this two ways, both optional and off by default:

**1. A new command, `HIMPORT`.** An explicit bulk-import API. A client first
names a field set with `PREPARE` (sending the field names **once**), then creates
keys that reuse it with `SET`, sending **only the values**:

```
# PREPARE <fieldset-name> <field> ...  -- name a reusable set of field names
HIMPORT PREPARE u name email age

# SET <key> <fieldset-name> <value> ...  -- create a key from that field set
HIMPORT SET user:1 u alice a@example.com 30   # user:1 = {name: alice, email: ..., age: 30}
HIMPORT SET user:2 u bob   b@example.com 25   # user:2, same fields, new values
```

The field names are sent once (in `PREPARE`); each `SET` carries only the key,
the field-set name, and the values.

`HIMPORT` is built for bulk hash ingestion: sending only the values cuts the
network traffic from clients to the master and the per-command work on the
server, versus an `HSET key f1 v1 …` per key. And because all these keys come
from one fixed field set, Redis takes that as a hint and stores them
template-encoded, so the same path also reduces memory.

**2. For workloads that don't use the command.** New configs let you turn
existing hashes into template-encoded ones with no code change, which is handy
when upgrading a dataset. Conversion happens during RDB load or at runtime as
hashes are written (e.g. via `HSET`). See the configuration section.

## When templates help, and when they don't

**Works well when:**

- **Many keys share the same, mostly stable field set**, classic object-mapping
  (one hash per user / order / session). The more keys share a field set, the
  bigger the saving.
- **Reads and value updates are as fast as on a regular hash**, reading any
  field (`HGET`, `HGETALL`, `HRANDFIELD`, …) and overwriting an existing field's
  value (`HSET key f v` where `f` already exists).

**Not a good fit when:**

- **Field names change often.** Because templates are immutable, `HSET` of a
  *new* field or `HDEL` detaches the key and re-resolves a template for the new
  field set (an O(field-count) lookup, creating one if needed). A workload that
  constantly adds/removes field names churns templates and erodes the benefit.
- **Field names are unique or highly dynamic per key.** With little sharing, a
  key ends up with its own template. A template allocates memory for some
  metadata besides the field list, so a template that is not shared by many keys
  may consume more memory than a regular hash.

**Out of scope: field expiration.** This first version targets mostly-stable
field sets, so it does not combine with per-field TTLs. If you add field
expiration (`HEXPIRE`) to a template-encoded key, the key is converted back to a
regular hash and loses the memory saving. That is the only effect.

---

# Public API

## Commands

### `HIMPORT` (since 8.10.0)

A container command for session-based bulk hash import. A **fieldset** is a
named, ordered list of field names scoped to the **client connection**: it is
never saved in RDB, never replicated, and not visible to other clients; it is
discarded when the connection closes or on `RESET`. It is only a *hint*, the
actual template is interned internally on first use.

```
HIMPORT PREPARE <fieldset-name> <field> [<field> ...]
```
Stores the ordered field-name list under `<fieldset-name>` for this connection
(overriding any previous fieldset with that name).
- **Reply:** `+OK`.
- **Errors:** `duplicate field name in fieldset` (a field name appears twice).

```
HIMPORT SET <key> <fieldset-name> <value> [<value> ...]
```
Creates or overwrites `<key>` as a hash whose fields come from the prepared
fieldset, paired positionally with the supplied values. The value count must
equal the fieldset's field count. On the first use of a given field set, Redis
interns it as a template; later keys reuse it.
- **Reply:** `+OK`.
- **Errors:** `no such fieldset` (name not prepared on this connection);
  `value count does not match fieldset field count`.

```
HIMPORT DISCARD <fieldset-name>
```
Removes the fieldset from the connection. Does **not** affect templates already
created or keys already written.
- **Reply:** `1` if removed, `0` if no such fieldset.

```
HIMPORT DISCARDALL
```
Removes every fieldset held by the connection.
- **Reply:** the number of fieldsets removed.

> There is no public command to read, name, or manage templates: they are an
> internal encoding, not part of the data model. (An internal `HSETC` command
> exists only for replication/AOF; see *Replication* under Internals.)

## Configuration

All settings are `MODIFIABLE_CONFIG` and default to `0` (off). They are applied
**lazily** like `hash-max-listpack-entries`; an existing hash is only re-encoded
on its next write, never immediately on `CONFIG SET`.

**Write-path auto-conversion** (for existing workloads, no code change). Why:
let large hashes created with ordinary `HSET`/`HMSET`/… gain the memory savings
transparently.

| Config | Meaning |
|---|---|
| `hash-min-template-entries` | A hash with at least this many fields is auto-converted to a template on its next write. `0` disables it. |
| `hash-max-template-entries` | Upper bound of the auto-convert window: hashes wider than this are left plain (keeps very wide hashes out of the shared registry). `0` = no upper bound. |

Conversion is one-directional: a hash converts **up** when it crosses the
minimum; shrinking does not auto-revert (it moves to a smaller template, never
back to a plain hash; see *Mutating a template-backed hash*). Hashes using field
expiration (`HEXPIRE`) are never converted.

**RDB-load-time conversion** (for upgrading existing datasets). Why: an RDB saved
before templates existed carries plain hashes; these let Redis materialize them
template-backed *as they load*, so an upgrade reclaims memory without rewriting
data. They act **only** while loading an RDB that contains no templates; if the
RDB already has templates, it loads as-is and these are ignored.

| Config | Meaning |
|---|---|
| `hash-rdb-load-min-template-entries` | Min field count to convert a plain hash to a template during load. `0` disables it. |
| `hash-rdb-load-max-template-entries` | Upper bound for the load-time window. `0` = no upper bound. |
| `hash-rdb-load-template-disassembly-threshold` | After load, a converted template kept only if shared by at least this many keys; templates below it are disassembled back to plain hashes (a few-key template wastes memory). Also acts as a safety valve: if too many below-threshold templates pile up mid-load, new template creation stops. `0` disables it (keep every converted template). |

## Observability

- **`OBJECT ENCODING <key>`** reports `template-listpack` or `template-array` for
  template-backed hashes (alongside the usual `listpack` / `hashtable`).
- **`INFO stats`:**
  - `hash_templates`, number of distinct templates in the registry.
  - `hash_template_keys`, total keys backed by a template.
- **`INFO memory`:** `used_memory_hash_templates`, bytes held by the shared
  template registry (counted as overhead, like `used_memory_functions`).
- **`MEMORY STATS`:** `hash.templates`, the same figure.

The shared template is **not** attributed to any single key: `MEMORY USAGE <key>`
reports a key's marginal cost (its values plus the template reference), while the
once-per-server schema cost shows up in the figures above.

---

# Internals (maintainers / agents)

Everything below is implementation detail, not a user contract.

## Templates & the registry

In the code a template is a `hashTemplate`: an **immutable**, **sorted** list of
field names with a small runtime **id**. Templates are **shared** across keys,
**reference-counted** (freed at zero refs), and **deduplicated**: two keys with
the same field-name set (in any order) share one template. A field lookup within
a key is a binary search over the sorted names (`hashTemplateFieldIndex`).

`hashTemplates` (`server.htemplates`, `src/server.h`) is the registry, with three
indexes onto the same templates:

- `by_fields`, keyed by a **commutative** field-set hash (Σ per-field siphash),
  so HSET/HDEL can update the bucket key in O(1) when one field changes (the full
  field list is still compared on a match).
- `by_id`, dense small-int id → template, for `TMPL_LP` (which stores the id).
- `by_fields_lp`, serialized field-name blob → template, so a self-contained
  `RESTORE` resolves its template in one O(1) lookup.

## Encodings

Two new object encodings (`src/object.h`), opaque like `listpack`/`hashtable`:

| Encoding | id | `o->ptr` layout | Used when |
|---|---|---|---|
| `OBJ_ENCODING_TMPL_LP` | 14 | listpack `[template_id (varint)][value0]…[valueN-1]` | values fit listpack limits |
| `OBJ_ENCODING_TMPL_ARRAY` | 15 | `hashTemplateArray { hashTemplate *tmpl; sds values[]; }` | a value/field-count exceeds listpack limits |

Field names live only in the template, never in the object. `TMPL_LP` is the
compact form (id is a 1–2 byte varint); `TMPL_ARRAY` embeds the template pointer
directly. The listpack thresholds are the usual `hash-max-listpack-entries` /
`hash-max-listpack-value`.

## Reference counting & lifetime

Two counts keep a template alive:

- **`key_refcount`** (atomic), number of live keys. Atomic because a BIO
  lazyfree thread may free a key off the main thread.
- **`hold_refcount`**, non-key holders: a connection's `HIMPORT PREPARE`
  fieldset, an in-progress RDB load, the `hsetc_cache`.

When both reach zero the template is reclaimed. A BIO thread that drops the last
key can't touch the registry, so it enqueues the template **id** into
`pending_free_ids` under `htemplates->lock`; the main thread drains it in
`hashTemplateDrainPendingFree` (from `serverCron`), re-checks both counts, and
frees. `htemplates->lock` guards only `pending_free_ids` + `by_id`; the registry,
`hold_refcount`, and propagation argv are main-thread-only. (This is the most
safety-critical path; it had a concurrency UAF fixed during development.)

## Encoding transitions

`hashTypeConvert(db, o, enc)` handles all conversions:

- **plain → template:** `hashTypeTryConvertToTemplate()` when enabled (config or
  HIMPORT), `TMPL_LP` if values fit a listpack, else `TMPL_ARRAY`.
- **`TMPL_LP` → `TMPL_ARRAY`:** when a value crosses `hash-max-listpack-value`,
  the field count crosses `hash-max-listpack-entries`, or the template can't use a
  listpack.
- **template → plain:** RDB-load disassembly of few-key templates (see
  *Persistence*).

## Mutating a template-backed hash

Templates are immutable, so `HSET` of a **new** field or `HDEL` **detaches** the
key from its current template and attaches it to a (new or existing) template for
the updated field set, **exact-match** only. The commutative field-hash makes
finding/creating that neighbor template cheap. Consequences:

- Once a key is template-backed it stays template-backed after mutation (it just
  moves between templates); `HDEL` of the last field deletes the key.
- This is deterministic, so replicas reproduce it identically.
- There is no partial / best-fit matching and no "leave removed fields empty"
  representation.

## Persistence

Layouts are deterministic across restart / replication / migration: a key's
reduced footprint is preserved everywhere.

**RDB** (`src/rdb.h`) stores the registry once at the top via opcode
`RDB_OPCODE_HASH_TEMPLATES` (242), then keys in compact **ref** form. Two variants
per encoding:

| Type | id | Form | Used by |
|---|---|---|---|
| `RDB_TYPE_HASH_TMPL_LP` | 29 | self-contained: `[count][f0]…[fN-1][lp_blob]` | DUMP |
| `RDB_TYPE_HASH_TMPL_LP_REF` | 30 | ref: raw lp blob (first entry = id) | RDB save |
| `RDB_TYPE_HASH_TMPL_ARRAY` | 31 | self-contained: `[count][f0][v0]…` | DUMP |
| `RDB_TYPE_HASH_TMPL_ARRAY_REF` | 32 | ref: `[id][v0]…[vN-1]` | RDB save |

On load, `rdbLoadHashTemplates` rebuilds the registry (each template hold-ref'd);
`rdbClearHashTemplates` releases those refs and is wired into
`rdbLoadRioWithLoadingCtx` so it covers **every** load path (disk startup,
diskless replica, AOF rdb-preamble); missing this caused "Duplicate hash
template ID" crashes on the second resync/loadaof, fixed during dev. RDB load can
also template/disassemble using the `hash-rdb-load-*` knobs, tracked by a per-load
`rdbLoadTemplateCtx`.

**DUMP / RESTORE** uses the **self-contained** types (29/31): field names are
inlined so the payload is portable, but it is tagged so the loader rebuilds a
*template-backed* hash (preserving the memory optimization).

**AOF** rewrite emits the keys so replay reconstructs the template encoding.

## Replication (the internal `HSETC` command)

Replication is unchanged except for **one internal command**. `HIMPORT SET`
propagates to replicas/AOF as `HSETC`, flagged `INTERNAL | NOSCRIPT`, never
issued by users. It tells the consumer the key is eligible for template-based
compaction, so the replica/migration destination rebuilds the same encoding.
`HSETC key f0…fN-1 v0…vN-1` carries the full field list plus values; a per-client
`hsetc_cache` skips the registry lookup on repeated same-template applies. It is
an implementation detail of propagation, intentionally not documented as API.

## Cluster / slot migration

Template-backed keys migrate via the self-contained DUMP/RESTORE form (field
names inlined) and are rebuilt with a fresh local template id on the destination,
which is a live node with its own id namespace.

## Memory accounting (implementation)

`used_memory_hash_templates` / `MEMORY STATS hash.templates` is an **O(1)
incremental counter** (`total_mem_size`, `±=` each template's `mem_size` on
create/free) plus the O(1) overhead of the lookup dicts and the by_id array, not
a registry walk, since there may be ~100k templates and `INFO` is polled often.

## Concurrency summary

`key_refcount` is the only off-main-thread field (atomic); its zero-transition
only enqueues an id under the lock. `htemplates->lock` guards `pending_free_ids` +
`by_id`. Registry mutation, `hold_refcount`, propagation argv, and template
revival (0→1) are main-thread-only. A `TMPL_LP` free may run on a BIO thread
(`hashTemplateLpFree`, resolving id → template under the lock).

## Tests

`tests/unit/type/hash-templates.tcl` (multiple `start_server` blocks for the
different config regimes) covers both encodings end-to-end, large field counts
across the stack/heap boundary, HRANDFIELD, DUMP/RESTORE, repeated full resyncs
and AOF rdb-preamble loads, the BIO-free vs main-thread-drop lifecycle fuzzer,
active defrag, RDB-load disassembly + alloc-size histogram consistency, and
INFO/MEMORY STATS reporting. Integration coverage in
`tests/integration/replication.tcl` and `tests/unit/dump.tcl`.

## Files changed

- `src/t_hash.c`, registry, templates, refcounting, encodings, conversions,
  `HIMPORT`/`HSETC`, RDB-load context, HRANDFIELD, memory accounting (bulk of it).
- `src/server.h`, template/registry/array structs, `redisMemOverhead`, prototypes.
- `src/object.h` / `src/object.c`, encodings, encoding reporting, memory wiring.
- `src/rdb.c` / `src/rdb.h`, RDB types, registry opcode, save/load, DUMP/RESTORE,
  load-time disassembly.
- `src/aof.c`, AOF rewrite.
- `src/config.c`, the auto-conversion configs.
- `src/db.c`, per-slot allocation-size histogram interaction.
- `src/commands/hsetc.json`, `src/commands/himport*.json` (+ `commands.def`).
- tests as above.
