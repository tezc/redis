# Hinted Hash Templates

Template-based hash encoding lets many keys share a single field-name layout,
reducing per-key memory overhead while preserving full hash semantics.

## Commands

### HIMPORT PREPARE

```
HIMPORT PREPARE <fieldset-name> field [field ...]
```

Declares a session-local **fieldset**: a named, ordered list of field names
that can be reused by subsequent `HIMPORT SET` calls on the same connection.
The fieldset lives only for the duration of the client connection. When the
client disconnects, all its fieldsets are automatically discarded. It overrides
any previous fieldset with the same name.

**Reply**

- Simple string reply: `OK`.
- Error reply: `duplicate field name in fieldset` if any field name appears
  more than once in the argument list.

---

### HIMPORT SET

```
HIMPORT SET key <fieldset-name> value [value ...]
```

Creates or overwrites *key* as a hash whose fields come from the previously
prepared *fieldset-name*, paired positionally with the supplied values. The
number of values must match the fieldset's field count. The resulting hash is
stored using a template-based encoding.

**Reply**

- Simple string reply: `OK`.
- Error reply: `no such fieldset` if *fieldset-name* has not been prepared on
  this connection.
- Error reply: `value count does not match fieldset field count` if the
  number of values differs from the fieldset's field count.

---

### HIMPORT DISCARD

```
HIMPORT DISCARD <fieldset-name>
```

Drops the named fieldset from the current connection. Existing keys that were
created with it are unaffected; only future `HIMPORT SET` lookups for this
name will fail.

**Reply**

- Integer reply: `1` if the fieldset was removed, `0` if no fieldset with
  this name was registered (silent no-op).

---

### HIMPORT DISCARDALL

```
HIMPORT DISCARDALL
```

Drops every fieldset currently held by this connection. Equivalent to issuing
`HIMPORT DISCARD` for each prepared fieldset.

**Reply**

- Integer reply: the number of fieldsets that were removed (`0` if none were
  registered).


## Config

### hash-min-template-entries

```
hash-min-template-entries <count>   (default: 0)
```

Enables automatic template encoding for hashes that are **not** created via
the `HIMPORT` API. When set to a positive value, any hash whose field count
reaches this threshold is converted to a template-based encoding on the fly:

- on `HSET` insert (and other field-adding paths) once the count crosses the
  threshold,
- on RDB load when the loaded hash already has at least this many fields.

If a template-based hash later drops below the threshold (e.g. via `HDEL`),
it is automatically converted back to `listpack` or `hashtable`.

Default is `0`, which disables this behavior — hashes only become
template-based when explicitly created with `HIMPORT SET`.

---

## Introspection

### Info output

The `INFO stats` section exposes two server-wide counters:

- `hash_templates` — number of distinct templates currently held in the
  shared registry. 
- `hash_template_keys` — total number of keys whose hash is backed by a
  template

---

### OBJECT ENCODING

```
OBJECT ENCODING key
```

Reports the underlying hash encoding. The possible values for hashes are:

**Regular hashes** (created with `HSET`, `HMSET`, etc.):

- `listpack` — compact contiguous layout used while the field count is at
  most `hash-max-listpack-entries` (default `512`) and every field/value is at
  most `hash-max-listpack-value` bytes (default `64`).
- `hashtable` — full hash table; used once the hash exceeds either listpack
  thresholds

**Template-based hashes** (created with `HIMPORT SET`, or auto-converted via
`hash-min-template-entries`):

- `template-listpack` — compact contiguous layout used while the field count is at
  most `hash-max-listpack-entries` (default `512`) and every value is at
  most `hash-max-listpack-value` bytes (default `64`).
- `template-array` — values stored in a plain array, chosen when the listpack constraints above are not met.






