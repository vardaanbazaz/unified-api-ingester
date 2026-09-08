# Framework Vision

## Goal

`unified-ingest` is a pluggable, unified ingestion framework capable of
handling diverse API types — varying auth patterns, pagination styles, and
response shapes. It is not a single-API brewery pipeline.

OpenBreweryDB is the first proof-of-concept source, not the project itself.

## v1 Scope

Three sources, deliberately chosen to be maximally different from each other:

1. **OpenBreweryDB** (already built) — no auth, offset/limit pagination, flat
   JSON records.
2. **Hugging Face Inference API — sentiment analysis**
   (`distilbert/distilbert-base-uncased-finetuned-sst-2-english`, called via
   `router.huggingface.co/hf-inference/models/<owner>/<name>`) — API-key auth,
   single-request (no pagination), nested JSON: a list containing one list of
   `{label, score}` objects, one per class
   (`[[{"label": "POSITIVE", "score": 0.9998767375946045}, {"label":
   "NEGATIVE", "score": 0.0001232026843354106}]]`).
3. **Twitch API** — OAuth2 (Client ID + Client Secret via `.env`, already
   confirmed working, no card required), cursor-based pagination, nested
   JSON.

### Domain fit and OAuth2 slot research

The brief's suggested domains were medical, AI/ML, and general IT/ops.
Hugging Face lands squarely in AI/ML. For the OAuth2 slot, a free, card-free,
`client_credentials`-based API in the medical or general-IT/ops space could
not be found:

- **GitLab's REST API** does not yet ship `client_credentials` support — it's
  an open feature request as of this writing.
- **Auth0's** free tier is ambiguous about requiring card verification.
- **Twitch's Helix API** is the one verified as free, no-card, and a genuine
  `client_credentials` flow, so it fills the OAuth2 slot at the cost of
  domain fit. This is a deliberate trade-off, flagged rather than hidden — see
  the self-review at the end of this doc. A medical or IT/ops OAuth2 source
  can replace it in a later version without changing anything about why it's
  in scope: the axis it stress-tests, not the industry it's from, is what
  matters for v1.

A cursor-paginated, no-auth medical source (**ClinicalTrials.gov API v2**,
confirmed free/public/no-key, `pageToken`/`nextPageToken` pagination) was
considered for the medical slot but not used, because its auth (`none`)
duplicates OpenBreweryDB's and the auth axis is the one axis where having 3
distinct values matters most. It's a strong candidate to add in a later
version once the interface has 3 real implementations to check against,
precisely because its pagination and shape (deep nesting under
`protocolSection`) differ from all three v1 sources.

**Free/public check (as verified during research):**

- OpenBreweryDB: fully public, no key, no signup. Already in use.
- Hugging Face Inference API: free tier confirmed, no credit card required —
  sign up for a free account, generate a token from account settings.
  Rate-limited (free tier is intended for prototyping, not production
  volume), which is fine for v1 testing.
- Twitch Helix API: free, requires registering an application in the Twitch
  Developer Console under a Twitch account (a signup step) to get a Client
  ID/Secret. No payment involved — confirmed working via `.env`, no card
  required.

### Deferred, named so it isn't lost

**Hugging Face Named Entity Recognition** (`dslim/bert-base-NER`) — a known
structurally-different HF task (list of typed entity objects with additional
fields). Explicitly deferred from v1. Not to be built now. Named here so it
isn't lost or accidentally scope-crept into v1.

### Out of scope for v1

Named explicitly so scope creep has something to check against:

- OAuth2 token refresh flows — v1's OAuth2 adapter (Twitch) acquires one
  `client_credentials` token per run and uses it until the run finishes.
  Mid-run expiry/refresh handling is out of scope until a source with
  long-running extraction and short-lived tokens actually requires it.
- Streaming responses — all v1 sources are extracted via ordinary buffered
  `requests` calls (whole-response-in-memory), matching the existing
  `APIExtractor` model. Newline-delimited JSON, chunked transfer encoding, or
  websocket-based sources are out of scope.
- Image/binary input handling
- Dynamic / self-describing schema inference — every source's schema is
  hand-declared (see "Relationship to `src/schema.py`" below), never
  inferred from a sample response at runtime. This is a direct consequence
  of the goal statement's "not zero-config magic" boundary, not a separate
  decision.
- Rate-limit-aware throttling beyond existing retry/backoff — the current
  `Retry`/`backoff_factor` mechanism in `APIExtractor._create_session`
  (status-code-triggered retry with exponential backoff) is preserved and
  reused as-is. Proactive rate-limit budgeting (e.g., reading
  `X-RateLimit-Remaining` headers and pacing requests ahead of a 429) is out
  of scope for v1.
- Building a generalized `BaseExtractor`/`BaseSchema` interface (that comes
  later, after all three concrete adapters exist — see Sequencing below)

## Relationship to the existing `src/schema.py`

Today, `src/schema.py` defines exactly one `COLUMN_SCHEMA` (a
`List[ColumnDef]`) for the `breweries` table, plus everything derived from
it: `DDL_COLUMNS`, `INSERT_COLUMNS`, `UPDATE_COLUMNS`,
`TRANSFORM_COLUMN_TYPES`, and `build_ddl()`. That shape — one ordered list of
`ColumnDef` entries as the single source of truth, with DDL/INSERT/UPDATE/
transform-type-map all mechanically derived from it — is the result of the
recent schema refactor and should be **preserved as the shape of a schema
definition**, not redesigned. It's a good design: one place to add a column,
everything else derives without hand-maintenance, and the `primary_key` flag
on `ColumnDef` (rather than string-matching a column name) is exactly the
kind of "declare intent, derive behavior" pattern the rest of the framework
should copy.

What changes going from one source to three is **cardinality, not shape**:
`schema.py` becomes per-source rather than a single fixed module.
Concretely, the migration plan is:

- Each adapter owns its own schema definition — e.g.
  `src/adapters/breweries/schema.py`, `src/adapters/huggingface/schema.py`,
  `src/adapters/twitch/schema.py` — each built from the same `ColumnDef` /
  `build_ddl` / `INSERT_COLUMNS` / `UPDATE_COLUMNS` pattern as today's
  `src/schema.py`.
- The generic loader (the framework-core equivalent of today's
  `DuckDBLoader`) stops importing a fixed `schema` module and instead takes
  a schema object as a constructor argument or parameter — i.e., "a schema
  object passed into a generic loader," not "the loader imports whichever
  schema module happens to be on disk." This is what actually makes the
  loader source-agnostic; without it, `load.py` stays coupled to one table
  no matter how many adapters exist. (Step 4 of Sequencing, below.)
- `ColumnDef`, `build_ddl`, `INSERT_COLUMNS`, and `UPDATE_COLUMNS` themselves
  likely get promoted into a shared framework-core module (e.g.
  `src/core/schema.py`) that each per-source schema file imports and
  instantiates from — so the *pattern* is defined once, and each source only
  supplies its own `COLUMN_SCHEMA` list and table name. This is the part
  most likely to need adjustment once a second real schema exists — for
  instance, Hugging Face's nested output may need a documented convention
  for flattening into columns, which the current `ColumnDef` shape doesn't
  yet have an opinion on.

What does **not** change: the principle that column order and identity live
in one declared list per table, that DDL/INSERT/UPDATE column lists are
derived rather than hand-duplicated, and that primary-key-ness is a flag on
the column, not a string comparison scattered through the loader.

## High-level adapter interface sketch

This is a shape, not implementation code. It's derived from comparing what
`APIExtractor` (`src/extract.py`) does today against what Hugging Face's and
Twitch's actual request models will require — not from a general theory of
REST APIs.

**Confidently shared today** (present in `APIExtractor` and clearly needed by
any HTTP-based source):

- A configured `requests.Session` with retry/backoff mounted on it
  (`_create_session` — status-code-triggered retries, exponential backoff).
  This has no source-specific logic in it at all; it's pure infrastructure.
- Timeout and max-retries as constructor-level configuration, sourced from a
  config object with per-source overrides.
- A `fetch_all()`-style entry point that returns "all records this call
  should produce" as the adapter's single public extraction method, so
  `main.py`'s orchestration doesn't need to know how any given source
  paginates internally.
- Logging shape (page/record counts, errors) at the same granularity as
  today's `logger.info`/`logger.error` calls.

**Guessed-at-being-shared** (plausible, but unverified until Hugging Face and
Twitch adapters actually exist — flagged here explicitly rather than
asserted):

- A `fetch_page(...)`-style single-page method as a separate, testable unit
  from `fetch_all()`. This shape assumes pagination is meaningful for every
  source; Hugging Face's adapter has no "page" concept at all (one call in,
  one result out), so either `BaseExtractor` makes this method optional, or
  pagination-aware sources implement it as an internal detail that isn't
  part of the shared interface. **Not resolved by this doc — resolved by
  writing the Hugging Face adapter and seeing what actually falls out.**
- An `authenticate()` or token-acquisition step as a distinct lifecycle
  method, separate from `fetch_*`. OpenBreweryDB has no such step; Hugging
  Face's is a static header; Twitch's is a stateful pre-flight HTTP call
  producing a token with its own expiry. Whether these three collapse into
  one interface method or need to stay adapter-specific internal detail is
  genuinely unknown until Twitch's adapter is written.
- Response-shape normalization (turning a nested Hugging Face or Twitch
  payload into the flat-ish row shape `transform.py` currently expects) as
  either a `BaseExtractor` responsibility or a separate per-source
  `flatten()`/`normalize()` step closer to `transform.py`. This boundary is
  speculative — it could reasonably live in either layer, and only writing
  both adapters will show which placement avoids duplication.
- Whether pagination style (offset-limit vs. cursor vs. none) is expressed as
  a strategy object injected into a single `BaseExtractor`, vs. three
  different subclasses each owning their own loop, is unresolved. Guessing
  at this now, before Twitch's cursor logic exists concretely, risks
  designing an abstraction around an imagined "pagination strategy" concept
  that may not match how Twitch's `pagination.cursor` actually needs to be
  threaded through requests.

The honest summary: the retry/session/config/logging layer is real, verified
shared ground. Everything about *how a page is fetched, how auth is
acquired, and how a response is shaped* is a hypothesis until it's been
checked against two real non-trivial adapters, not one.

## Sequencing

Do not build out of order:

1. Build the Hugging Face sentiment adapter as its own standalone concrete
   class, duplicated/ugly where needed, sitting alongside the existing
   `APIExtractor` — no shared base class yet.
2. Build the Twitch adapter the same way — standalone, concrete.
3. Only once all three adapters exist side by side, compare them and extract
   `BaseExtractor`/`BaseSchema` from the real diff between them — not
   designed upfront from imagination.
4. Only after that, revisit `schema.py` to decide whether schema becomes a
   constructor argument (schema-as-data) rather than a fixed import — this
   decision is cheap with three real examples in hand and expensive to guess
   at now.

This mirrors the discipline in `docs/schema_refactor_plan.md`: that refactor
only extracted `schema.py`'s shared shape after `load.py` and `transform.py`
already had duplicated, concrete column lists to diff against — it didn't
design `ColumnDef`/`build_ddl` from first principles before either consumer
existed. The same applies here at a larger scope: two real, duplicated
adapters first, then one real extraction pass, not an interface imagined in
advance of any second example.

## Self-review

- **Goal statement:** stated what "unified" means (shared core + adapter
  pattern) and explicitly what it does not mean (no zero-config/auto-onboarding
  magic). ✅
- **v1 scope:** three named APIs (OpenBreweryDB, Hugging Face Inference API,
  Twitch Helix API) with concrete auth/pagination/shape per source,
  justification for each as a stress test, and a free/public check for all
  three. ✅ Twitch's Client ID/Secret flow via `.env` is now confirmed
  working with no card required, resolving the open question this section
  originally flagged.
- **Domain fit:** two of three named target domains (AI/ML via Hugging Face;
  general/business via the existing brewery source) are hit directly. The
  medical and general-IT/ops domains are not directly represented in v1 —
  a free, card-free, `client_credentials`-OAuth2 API was sought in those
  domains specifically (GitLab's API — `client_credentials` support isn't
  shipped yet, only proposed — and Auth0 — free tier exists but
  card-verification requirement was ambiguous in what was found) and Twitch
  was used instead, since it was the one that could be verified cleanly.
  **This is a guess-flagged trade-off, not a verified best fit** — a
  domain-matched OAuth2 source should be re-checked before treating Twitch
  as permanent rather than a stand-in for the OAuth2 axis.
- **Non-goals:** named and explicitly tied to why each is deferred (OAuth2
  refresh — v1's token is acquire-once-per-run; streaming — all sources use
  buffered whole-response requests; schema inference — direct consequence of
  the goal statement's non-magic boundary; rate-limit-aware throttling —
  existing status-code retry/backoff is reused as-is, proactive budgeting is
  separate work). ✅
- **`schema.py` relationship:** explicit recommendation (per-adapter schema
  files, `ColumnDef`/`build_ddl`/`INSERT_COLUMNS`/`UPDATE_COLUMNS` pattern
  promoted to a shared module and reused, not redesigned; generic loader
  takes a schema object as a parameter rather than importing a fixed
  module). ✅ **Flagged:** how Hugging Face's nested output maps onto a
  `ColumnDef` list at all is unresolved and called out as a likely point of
  friction, not silently assumed to work.
- **Adapter interface sketch:** shared-today list (session/retry/timeout/
  config/logging) is grounded in what `src/extract.py` actually does, not
  guessed. Four specific interface questions (page-fetch as a separate
  method, auth as a lifecycle step, response normalization's layer,
  pagination-strategy shape) are explicitly marked speculative pending a
  second and third real adapter. ✅
- **Sequencing:** confirmed as build-second-adapter-concretely →
  build-third-adapter-concretely → extract shared interface from the diff,
  explicitly mirrored against how `docs/schema_refactor_plan.md` did the
  same at the schema-file scope. ✅
- **Where guessing was necessary rather than verification:** (1) every
  bullet under "guessed-at-being-shared" in the adapter interface section,
  (2) that a domain-matched (medical/IT-ops), free, card-free,
  `client_credentials` OAuth2 API exists at all — none was found in the time
  spent researching this doc, so absence-of-evidence is reported rather than
  treated as proof none exists.

## Repo Decision

This repo (`unified-ingest`, renamed from `api-ingestor`) is evolving in
place — not forked — so the existing commit history (CI gate fix, schema
refactor) reads as a continuous story of this framework's development, not
orphaned in an old repo.
