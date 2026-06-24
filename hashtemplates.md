# Hinted Hash Templates

> Status: in development on branch `hinted-hash-dev` (feature starts at commit
> `f9036783d`). Target version: **8.10.0**. This document is both the feature
> reference and the PR description.

## Summary

This PR introduces **Hinted Hash Templates**, a way to cut the memory used by
hashes that share the same field names.

When many hashes have the same layout (for example one hash per user, each with
`name`, `email`, `age`), every key normally stores its own copy of those field
names. With this new feature, the shared field-name set is stored **once**, in an
internal *template*, and each key keeps only its values plus a small reference to
it. Redis creates, shares (reference-counted), and frees templates automatically.

Templates are an **internal encoding, not exposed to users**: for most cases,
users may not tell the difference. Existing hash commands keep working exactly as
before, with the same semantics and replies; the hash just uses less memory. The
application never creates or sees a template.

The PR exposes this two ways, both opt-in:

**1. A new command, `HIMPORT`.** An explicit bulk-import API for hashes. A client
first names a field set with `PREPARE` (sending the field names **once**), then creates
keys that reuse it with `SET`, sending **only the values**:

```
# PREPARE <fieldset-name> <field> ...  -- name a reusable set of field names
HIMPORT PREPARE u name email age

# SET <key> <fieldset-name> <value> ...  -- create a key from that field set
HIMPORT SET user:1 u alice a@example.com 30   # user:1 = {name: alice, email: ..., age: 30}
HIMPORT SET user:2 u bob   b@example.com 25   # user:2, same fields, new values
HIMPORT SET user:3 u carol c@example.com 41
...
```

The field names are sent once (in `PREPARE`); each `SET` carries only the key,
the field-set name, and the values.

`HIMPORT` is built for bulk hash ingestion: sending only the values cuts the
network traffic from clients to the master and the per-command work on the
server, versus an `HSET key f1 v1 …` per key. And because all these keys come
from one fixed field set, Redis takes that as a hint and stores them
template-encoded, so the same path also reduces memory.

**2. Automatic conversion, for workloads that don't use the command.** Not every
application can adopt a new command, so this PR adds new configs that let Redis
convert eligible hashes to template encoding on its own, with no code change.
This happens on two paths:

- **At upgrade time:** when an old RDB (full of plain hashes) is loaded, its
  hashes are converted to templates as they load, so the dataset reclaims memory
  without being rewritten.
- **At runtime:** a hash is converted on its next write (e.g. via `HSET`) once it
  grows past a configurable field count.

Both paths are off by default; you enable them with field-count thresholds. See
the configuration section.

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
  field set (a registry lookup, creating and sorting a new template if the layout
  is new). A workload that
  constantly adds/removes field names churns templates and erodes the benefit.
  Also note that once a hash is template-encoded it stays that way for the life
  of the key: it moves between templates as fields change, but never reverts to
  a plain hash.
- **Field names are unique or highly dynamic per key.** With little sharing, a
  key ends up with its own template. A template allocates memory for some
  metadata besides the field list, so a template that is not shared by many keys
  may consume more memory than a regular hash.

> **Out of scope: hash field expiration.** This first version targets
> mostly-stable field sets, so a template-encoded hash cannot use hash field
> expiration. Adding it (`HEXPIRE`) to such a key converts it back to a regular
> hash and loses the memory saving. That is the only effect.

### Example saving

A local run: 2.8M hashes over 100 distinct field sets (so 100 templates), each
hash 20-50 fields (avg ~34), average field name ~13 bytes, average value
~13 bytes. 20% of the field sets contain one value larger than 64 bytes, so those
keys are hashtable/array-encoded and the rest stay listpack:

| Dataset | Encodings | Memory |
|---|---|---|
| Regular hashes | `listpack` + `hashtable` | 3.49 GB |
| Template-encoded | `template-listpack` + `template-array` | 1.77 GB |

About **49% less memory**. The saving tracks how much of each key is field names:
highest for the listpack keys (short values, names repeated per key) and lower
for the keys with a large value, where the value dominates the footprint.

---

# Public API

## Commands

### `HIMPORT` (since 8.10.0)

A container command for session-based bulk hash import. A **fieldset** is a
named, ordered list of field names scoped to the **client connection**: it is
not visible to other clients and is discarded when the connection closes or the
client issues the `RESET` command. A connection can prepare several fieldsets,
each under its own name, and
refer to them by name in later commands.

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
Creates `<key>` as a hash whose fields come from the prepared fieldset, paired
positionally with the supplied values. If `<key>` already exists it is replaced,
whatever its current type (like a plain `SET`). The value count must equal the
fieldset's field count. On the first use of a given field set, Redis interns it
as a template; later keys reuse it.
- **Reply:** `+OK`.
- **Errors:** `no such fieldset` (name not prepared on this connection);
  `value count does not match fieldset field count`.

```
HIMPORT DISCARD <fieldset-name>
```
Removes the fieldset from the connection.
- **Reply:** `1` if removed, `0` if no such fieldset.

```
HIMPORT DISCARDALL
```
Removes every fieldset held by the connection.
- **Reply:** the number of fieldsets removed.

## Configuration (automatic conversion to template encoding)

All settings default to `0` (off) and can be changed at runtime with `CONFIG SET`.

### On the write path (for live workloads)

These convert hashes created or modified by normal commands (`HSET`, `HMSET`, …),
so an existing application gains the memory saving with no code change. They take
effect **lazily**, like `hash-max-listpack-entries`: a change applies to a given
hash only on its next write, not the moment you run `CONFIG SET`.

| Config | Meaning |
|---|---|
| `hash-min-template-entries` | Minimum field count for a hash to be auto-converted to a template on its next write. `0` disables auto-conversion. |
| `hash-max-template-entries` | Maximum field count for auto-conversion: a hash wider than this is left a plain hash (keeps very wide hashes out of the shared registry). `0` means no upper bound. |

A hash is not converted if it uses hash field expiration, even when its field
count meets the minimum.

### On RDB load (for upgrading an existing dataset)

An RDB saved before this feature contains only plain hashes. These configs let
Redis convert them to templates *as the RDB loads*, so an upgrade reclaims memory
without rewriting data. They only apply to RDBs without templates; an RDB that already has templates is
loaded as-is, with no load-time conversion.

| Config | Meaning |
|---|---|
| `hash-rdb-load-min-template-entries` | Minimum field count to convert a plain hash to a template during load. `0` disables load-time conversion. |
| `hash-rdb-load-max-template-entries` | Maximum field count for load-time conversion. `0` means no upper bound. |
| `hash-rdb-load-template-disassembly-threshold` | Minimum number of keys a converted template must end up with to be kept. `0` keeps every converted template. |

The disassembly threshold avoids wasting memory on templates that end up shared
by only a few keys: at the end of the load, if a template is used by fewer keys
than the threshold, those keys are converted back to plain hashes and the
template is freed.

It also acts as a safety valve during the load. Redis tracks how many converted
templates are still below the threshold; if too many of these pile up (an RDB
with many distinct field sets, each used by only a few keys, is a poor fit for
templates), Redis stops creating new templates partway through the load. From
that point, only hashes whose field set matches a template already created during
this load are template-encoded; the rest stay plain.

## Observability

`OBJECT ENCODING <key>` reports `template-listpack` or `template-array` for
template-backed hashes, in addition to the usual `listpack` / `hashtable`.

Server-wide counters:

| Field | Where | Meaning |
|---|---|---|
| `hash_templates` | `INFO stats` | distinct templates in the registry |
| `hash_template_keys` | `INFO stats` | total keys backed by a template |
| `used_memory_hash_templates` | `INFO memory` | bytes held by the registry |
| `hash.templates` | `MEMORY STATS` | same as `used_memory_hash_templates` |

The shared template is **not** attributed to any single key: `MEMORY USAGE <key>`
reports only what that key actually owns (its values plus the template reference),
while the field names stored once in the shared template show up in the counters
above.

## Behavioral notes

- **`HSCAN`** on a template-encoded hash returns all fields in a single reply
  with cursor `0`, rather than incrementally across calls. This matches how Redis
  already scans a `listpack`-encoded hash (only `hashtable` encodings are scanned
  cursor-by-cursor), so a client that loops until the cursor is `0` works
  unchanged.
- **Field order** in `HGETALL` / `HKEYS` follows the template's internal order,
  not insertion order. Hash field order has never been guaranteed, so this is
  within spec, but it can differ from a regular hash for the same inserts.

---

# Internals (maintainers / agents)

Everything below is implementation detail, not a user contract.

## Templates & the registry

In the code a template is a `hashTemplate`: an **immutable** list of field names
with a small runtime **id**. The names are kept **sorted by `sdscmplen`** (length
first, then a byte compare). A template matches a field set **exactly**: two keys
share one template only if their field-name sets are identical (in any order), so
templates are **shared** and **deduplicated** across keys. A field lookup within a
key is a binary search over the sorted names (`hashTemplateFieldIndex`).

Because a template is immutable and exact-match, `HSET` of a **new** field or
`HDEL` **detaches** the key from its current template and re-resolves a template
for the new field set, creating a new one if none matches (which costs real work:
allocating the struct and copying and sorting the field names). Consequences:

- Once a key is template-backed it stays template-backed after mutation (it just
  moves between templates); `HDEL` of the last field deletes the key.
- This is deterministic, so replicas reproduce it identically.
- There is no partial / best-fit matching and no "leave removed fields empty"
  representation.

Finding the template for the changed field set stays relatively cheap thanks to
how the **registry** is keyed. The registry holds every live template, keyed by a
**commutative** field-set hash: the sum of the per-field siphashes. Because it is a sum, the order of the fields does not matter, and a
single field can be added or removed incrementally without rehashing the whole
set (`+= siphash(field)` on add, `-= siphash(field)` on remove). So `HSET`/`HDEL`
updates the lookup key in O(1) when one field changes, and the registry locates
the template in a single lookup, with a full field-list compare as the final step
to confirm the match.

## Encodings

Two new object encodings (`src/object.h`). Like the existing `listpack`/`hashtable`
encodings, the internal layout behind `o->ptr` is reached only through the hash
type accessors, not touched directly by the rest of the code:

| Encoding | id | `o->ptr` layout | Used when |
|---|---|---|---|
| `OBJ_ENCODING_TMPL_LP` | 14 | listpack `[template_id (varint)][value0]…[valueN-1]` | values fit listpack limits |
| `OBJ_ENCODING_TMPL_ARRAY` | 15 | `hashTemplateArray { hashTemplate *tmpl; sds values[]; }` | a value/field-count exceeds listpack limits |

`TMPL_LP` is the compact form (id is a 1–2 byte varint); `TMPL_ARRAY` embeds the
template pointer directly. The listpack thresholds are the usual
`hash-max-listpack-entries` / `hash-max-listpack-value`.

## Fieldsets (the `HIMPORT` fast path)

`HIMPORT PREPARE` does the per-layout work once: it sorts the field names, looks
them up in the registry (creating the template if the layout is new), takes a
reference on the template to keep it alive, and stores the template pointer on the
client as a *fieldset*. The template stores its fields sorted, but the caller
declares them in its own order, so the fieldset also remembers which declared
position each value maps to (so `HIMPORT SET` can drop positional values into the
right template slots). The template therefore exists as soon as `PREPARE` runs,
before any key uses it.

`HIMPORT SET` then just finds the fieldset by name and writes the key from the
cached template and that mapping: no registry lookup, no field sorting, no
per-call allocation for the layout. That is where the ingestion speedup comes
from.

## Reference counting & lifetime

Two counts keep a template alive:

- **`key_refcount`** (atomic), number of live keys. Atomic because a key can be
  freed off the main thread by a BIO lazyfree thread (lazyfree deletes, `FLUSH
  ASYNC`, or background trim after a slot migration).
- **`hold_refcount`**, non-key holders, such as a connection's `HIMPORT PREPARE`
  fieldset.

A template can therefore be alive with zero keys: right after `PREPARE` (before
any key uses it), or after all its keys are deleted while a fieldset still holds
it. It is freed once both counts reach zero. Since a key can be freed on a
background thread, the actual deletion of the template is deferred to the main
thread, which re-checks both counts before freeing it.

## Persistence

Layouts are deterministic across restart / replication / migration: a key's
reduced footprint is preserved everywhere.

**RDB (format change).** RDB writes the registry once at the top via opcode
`RDB_OPCODE_HASH_TEMPLATES`, then each templated key in a compact **ref**
form: only its values plus an integer id referencing a template from that
registry, with no field names repeated per key. Two variants per encoding:

| Type | id | Form | Used by |
|---|---|---|---|
| `RDB_TYPE_HASH_TMPL_LP` | 29 | self-contained: `[fields_lp_blob][values_lp_blob]` | DUMP |
| `RDB_TYPE_HASH_TMPL_LP_REF` | 30 | ref: raw lp blob (first entry = id) | RDB save |
| `RDB_TYPE_HASH_TMPL_ARRAY` | 31 | self-contained: `[count][f0][v0]…` | DUMP |
| `RDB_TYPE_HASH_TMPL_ARRAY_REF` | 32 | ref: `[id][v0]…[vN-1]` | RDB save |

On load the registry is rebuilt and then released on every load path (disk
startup, diskless replica, AOF rdb-preamble). Loading can also convert to or from
template encoding via the `hash-rdb-load-*` settings (see *Configuration*).

**DUMP / RESTORE** uses the **self-contained** types (`RDB_TYPE_HASH_TMPL_LP` =
29, `RDB_TYPE_HASH_TMPL_ARRAY` = 31): the field names are inlined so the payload
is portable. The leading RDB type byte is what tells the destination this is a
template hash, so `RESTORE` interns the inlined field set as a template and
rebuilds a *template-backed* hash rather than a plain one.

**AOF.** During normal operation the AOF receives the `HSETC` commands propagated
by `HIMPORT SET`. On rewrite it uses the RDB format (the types above), or, with
the RDB preamble disabled, emits one `HSETC` per template hash.

## Replication (the internal `HSETC` command)

`HIMPORT SET` propagates to replicas and the AOF as
`HSETC <key> <field1> ... <fieldN> <value1> ... <valueN>`, an internal-only
command, not exposed to users. All field names come first, then all values in the
same order. It carries the full field list plus
the values and marks the key as template-eligible, so a replica, sub-replica, or
slot-migration destination rebuilds the same template encoding.

Carrying the field names on every write is the simplest thing that works
uniformly across the AOF, replicas, sub-replicas, and atomic slot migration.
**TODO:** this is suboptimal, as it repeats the field names on every write; a
future version could send them once per template and then values only.

## Cluster / slot migration

Template-backed keys migrate via the self-contained DUMP/RESTORE form, which
carries the field names. From the RDB type byte the destination knows the key
must be template-backed, so it resolves the field set in its own registry
(creating the template if the layout is new) and rebuilds the key
template-encoded.

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
