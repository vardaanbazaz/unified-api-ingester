# Unified Ingestion Framework — Vision

**Status:** proposal / planning document, no code changes implied by this doc.
**Scope:** direction and v1 boundaries only. Implementation is sequenced separately (see "Sequencing recommendation" below), following the same discipline as `docs/schema_refactor_plan.md`.

---

## Goal statement

This repo starts as a single-API pipeline hardcoded to OpenBreweryDB. The goal is to generalize it into a framework that can onboard additional REST API sources through a **consistent adapter pattern**, so that adding a new source is a matter of writing a new adapter (extractor + schema + minimal wiring) rather than rewriting the extract/transform/load core each time. "Unified" specifically means: one shared ETL core, one shared loader/DDL mechanism, and one documented adapter interface that each source implements — with auth, pagination, and response-shape handling isolated inside each source's own adapter code so the core never needs to special-case a source.

"Unified" explicitly does **not** mean: automatic onboarding of an arbitrary API from a URL, zero-config schema inference, or a plugin system that works without a human writing an adapter. Every new source still requires a person to read that API's docs, write its extractor, and declare its schema. The framework's job is to make that person's work small and mechanical (implement a known interface, reuse the loader and retry/backoff logic) — not to eliminate the work.

---

## v1 scope: exactly which APIs

Three sources total, chosen to be maximally different from each other across auth mechanism, pagination style, and response shape — the three axes most likely to break an adapter interface designed from only one example.

| Source | Domain | Auth | Pagination | Shape |
|---|---|---|---|---|
| OpenBreweryDB (existing) | business directory | none | offset-limit (`page`, `per_page`) | flat |
| Hugging Face Inference API | AI/ML | API key (`Authorization: Bearer <token>`) | none (single call per request) | nested (task-dependent; e.g. a list-of-lists of `{label, score}` objects for classification tasks) |
| Twitch Helix API | streaming platform (see note below) | OAuth2 (`client_credentials` grant) | cursor (`pagination.cursor` field, passed back as `after`) | shallow-nested (mostly flat scalar fields per record, plus array-valued fields like `tag_ids`) |

**Why each is a good stress-test:**

- **OpenBreweryDB** (already integrated): baseline. No auth, numeric page/per-page pagination advanced until an empty page, and a flat list of scalar-field dicts. This is the "easy case" the current `APIExtractor` was built against — useful as the control, not a stress test on its own.
- **Hugging Face Inference API**: forces the adapter interface to support a mandatory credential header (not just an optional one), and a request model with **no pagination loop at all** — a single POST per unit of work, where "fetch all" doesn't mean "iterate until empty page" but "call once (or once per input) and stop." It also forces the schema layer to handle non-flat output (nested lists/dicts) rather than a flat row per element.
- **Twitch Helix API**: forces the adapter interface to support an actual OAuth2 token-acquisition step (a separate token endpoint hit before the data call, using `client_credentials`) and cursor-based pagination, where "next page" means "pass back an opaque cursor from the last response" instead of computing `page + 1` yourself. This is structurally incompatible with the current `fetch_page(page: int, ...)` signature, which is the point.

**Domain-fit note:** the brief's suggested domains were medical, AI/ML, and general IT/ops. Hugging Face lands squarely in AI/ML. For the OAuth2 slot, I could not find a free, card-free, `client_credentials`-based API in the medical or general-IT/ops space (see verification below) — GitLab's REST API does not yet ship `client_credentials` support (it's an open feature request as of this writing), and Auth0's free tier is ambiguous about requiring card verification. Twitch's Helix API is the one I could verify as free, no-card, and a genuine `client_credentials` flow, so it fills the OAuth2 slot at the cost of domain fit. This is a deliberate trade-off, flagged rather than hidden — see the self-review at the end of this doc. A medical or IT/ops OAuth2 source can replace it in a later version without changing anything about why it's in scope: the axis it stress-tests, not the industry it's from, is what matters for v1.

A cursor-paginated, no-auth medical source (**ClinicalTrials.gov API v2**, confirmed free/public/no-key, `pageToken`/`nextPageToken` pagination) was considered for the medical slot but not used, because its auth (`none`) duplicates OpenBreweryDB's and the auth axis is the one axis where having 3 distinct values matters most. It's a strong candidate to add in a later version once the interface has 3 real implementations to check against, precisely because its pagination and shape (deep nesting under `protocolSection`) differ from all three v1 sources.

**Free/public check (as verified during this doc's research):**

- OpenBreweryDB: fully public, no key, no signup. Already in use.
- Hugging Face Inference API: free tier confirmed, no credit card required — sign up for a free account, generate a token from account settings. Rate-limited (free tier is intended for prototyping, not production volume), which is fine for v1 testing.
- Twitch Helix API: free, but requires registering an application in the Twitch Developer Console under a Twitch account (i.e., a signup step) to get a Client ID/Secret. No payment involved. **Flag:** I did not find explicit confirmation of whether a credit card is required at account/app-registration time — this should be verified hands-on before Task 1 of implementation, not assumed from this doc.

---

## Explicit non-goals for v1

These are consciously deferred, not forgotten:

- **OAuth2 token refresh flows.** v1's OAuth2 adapter (Twitch) acquires one `client_credentials` token per run and uses it until the run finishes. Mid-run expiry/refresh handling is out of scope until a source with long-running extraction and short-lived tokens actually requires it.
- **Streaming / chunked responses.** All v1 sources are extracted via ordinary buffered `requests` calls (whole-response-in-memory), matching the existing `APIExtractor` model. Newline-delimited JSON, chunked transfer encoding, or websocket-based sources are out of scope.
- **Dynamic / self-describing schema inference.** Every source's schema is hand-declared (see next section), never inferred from a sample response at runtime. This is a direct consequence of the goal statement's "not zero-config magic" boundary, not a separate decision.
- **Rate-limit-aware throttling beyond existing retry/backoff.** The current `Retry`/`backoff_factor` mechanism in `APIExtractor._create_session` (status-code-triggered retry with exponential backoff) is preserved and reused as-is. Proactive rate-limit budgeting (e.g., reading `X-RateLimit-Remaining` headers and pacing requests ahead of a 429) is out of scope for v1.

---

## Relationship to the existing `src/schema.py`

Today, `src/schema.py` defines exactly one `COLUMN_SCHEMA` (a `List[ColumnDef]`) for the `breweries` table, plus everything derived from it: `DDL_COLUMNS`, `INSERT_COLUMNS`, `UPDATE_COLUMNS`, `TRANSFORM_COLUMN_TYPES`, and `build_ddl()`. That shape — one ordered list of `ColumnDef` entries as the single source of truth, with DDL/INSERT/UPDATE/transform-type-map all mechanically derived from it — is the result of the recent schema refactor and should be **preserved as the shape of a schema definition**, not redesigned. It's a good design: one place to add a column, everything else derives without hand-maintenance, and the `primary_key` flag on `ColumnDef` (rather than string-matching a column name) is exactly the kind of "declare intent, derive behavior" pattern the rest of the framework should copy.

What changes going from one source to three is **cardinality, not shape**: `schema.py` becomes per-source rather than a single fixed module. Concretely, the proposal is:

- Each adapter owns its own schema definition — e.g. `src/adapters/breweries/schema.py`, `src/adapters/huggingface/schema.py`, `src/adapters/twitch/schema.py` — each built from the same `ColumnDef` / `build_ddl` / `INSERT_COLUMNS` / `UPDATE_COLUMNS` pattern as today's `src/schema.py`.
- The generic loader (the framework-core equivalent of today's `DuckDBLoader`) stops importing a fixed `schema` module and instead takes a schema object as a constructor argument or parameter — i.e., "a schema object passed into a generic loader," not "the loader imports whichever schema module happens to be on disk." This is what actually makes the loader source-agnostic; without it, `load.py` stays coupled to one table no matter how many adapters exist.
- `ColumnDef`, `build_ddl`, `INSERT_COLUMNS`, and `UPDATE_COLUMNS` themselves likely get promoted into a shared framework-core module (e.g. `src/core/schema.py`) that each per-source schema file imports and instantiates from — so the *pattern* is defined once, and each source only supplies its own `COLUMN_SCHEMA` list and table name. This is the part most likely to need adjustment once a second real schema exists (see honesty note in the next section) — for instance, Hugging Face's nested output may need a documented convention for flattening into columns, which the current `ColumnDef` shape doesn't yet have an opinion on.

What does **not** change: the principle that column order and identity live in one declared list per table, that DDL/INSERT/UPDATE column lists are derived rather than hand-duplicated, and that primary-key-ness is a flag on the column, not a string comparison scattered through the loader.

---

## High-level adapter interface sketch

This is a shape, not implementation code. It's derived from comparing what `APIExtractor` (`src/extract.py`) does today against what Hugging Face's and Twitch's actual request models will require — not from a general theory of REST APIs.

**Confidently shared today** (present in `APIExtractor` and clearly needed by any HTTP-based source):

- A configured `requests.Session` with retry/backoff mounted on it (`_create_session` — status-code-triggered retries, exponential backoff). This has no source-specific logic in it at all; it's pure infrastructure.
- Timeout and max-retries as constructor-level configuration, sourced from a config object with per-source overrides.
- A `fetch_all()`-style entry point that returns "all records this call should produce" as the adapter's single public extraction method, so `main.py`'s orchestration doesn't need to know how any given source paginates internally.
- Logging shape (page/record counts, errors) at the same granularity as today's `logger.info`/`logger.error` calls.

**Guessed-at-being-shared** (plausible, but unverified until Hugging Face and Twitch adapters actually exist — flagged here explicitly rather than asserted):

- A `fetch_page(...)`-style single-page method as a separate, testable unit from `fetch_all()`. This shape assumes pagination is meaningful for every source; Hugging Face's adapter has no "page" concept at all (one call in, one result out), so either `BaseExtractor` makes this method optional, or pagination-aware sources implement it as an internal detail that isn't part of the shared interface. **Not resolved by this doc — resolved by writing the Hugging Face adapter and seeing what actually falls out.**
- An `authenticate()` or token-acquisition step as a distinct lifecycle method, separate from `fetch_*`. OpenBreweryDB has no such step; Hugging Face's is a static header; Twitch's is a stateful pre-flight HTTP call producing a token with its own expiry. Whether these three collapse into one interface method or need to stay adapter-specific internal detail is genuinely unknown until Twitch's adapter is written.
- Response-shape normalization (turning a nested Hugging Face or Twitch payload into the flat-ish row shape `transform.py` currently expects) as either a `BaseExtractor` responsibility or a separate per-source `flatten()`/`normalize()` step closer to `transform.py`. This boundary is speculative — it could reasonably live in either layer, and only writing both adapters will show which placement avoids duplication.
- Whether pagination style (offset-limit vs. cursor vs. none) is expressed as a strategy object injected into a single `BaseExtractor`, vs. three different subclasses each owning their own loop, is unresolved. Guessing at this now, before Twitch's cursor logic exists concretely, risks designing an abstraction around an imagined "pagination strategy" concept that may not match how Twitch's `pagination.cursor` actually needs to be threaded through requests.

The honest summary: the retry/session/config/logging layer is real, verified shared ground. Everything about *how a page is fetched, how auth is acquired, and how a response is shaped* is a hypothesis until it's been checked against two real non-trivial adapters, not one.

---

## Sequencing recommendation

Build the second adapter (Hugging Face) concretely first, duplicating `extract.py`/`transform.py`/`schema.py` code where needed rather than pre-abstracting a `BaseExtractor`. Then build the third adapter (Twitch) the same way — concretely, duplicating again rather than reusing a guessed interface. Only after both real adapters exist, diff the three implementations (breweries, Hugging Face, Twitch) and extract the shared `BaseExtractor`/schema-loader interface from what's *actually* common across the diff — not from the speculative bullets in the section above.

This mirrors the discipline in `docs/schema_refactor_plan.md`: that refactor only extracted `schema.py`'s shared shape after `load.py` and `transform.py` already had duplicated, concrete column lists to diff against — it didn't design `ColumnDef`/`build_ddl` from first principles before either consumer existed. The same applies here at a larger scope: two real, duplicated adapters first, then one real extraction pass, not an interface imagined in advance of any second example.

---

## Self-review

- **Goal statement:** stated what "unified" means (shared core + adapter pattern) and explicitly what it does not mean (no zero-config/auto-onboarding magic). ✅
- **v1 scope:** three named APIs (OpenBreweryDB, Hugging Face Inference API, Twitch Helix API) with concrete auth/pagination/shape per source, justification for each as a stress test, and a free/public check for all three. ✅ **Flagged:** Twitch requires app registration (a signup step) though no payment; I could not confirm from research whether that registration requires a credit card, and this should be checked hands-on before implementation starts.
- **Domain fit:** two of three named target domains (AI/ML via Hugging Face; general/business via the existing brewery source) are hit directly. The medical and general-IT/ops domains are not directly represented in v1 — I looked for a free, card-free, `client_credentials`-OAuth2 API in those domains specifically (checked GitLab's API — `client_credentials` support isn't shipped yet, only proposed — and Auth0 — free tier exists but card-verification requirement was ambiguous in what I found) and used Twitch instead, since it was the one I could verify cleanly. **This is a guess-flagged trade-off, not a verified best fit** — a domain-matched OAuth2 source should be re-checked before treating Twitch as permanent rather than a stand-in for the OAuth2 axis.
- **Non-goals:** four named and explicitly tied to why each is deferred (OAuth2 refresh — v1's token is acquire-once-per-run; streaming — all sources use buffered whole-response requests; schema inference — direct consequence of the goal statement's non-magic boundary; rate-limit-aware throttling — existing status-code retry/backoff is reused as-is, proactive budgeting is separate work). ✅
- **`schema.py` relationship:** explicit recommendation (per-adapter schema files, `ColumnDef`/`build_ddl`/`INSERT_COLUMNS`/`UPDATE_COLUMNS` pattern promoted to a shared module and reused, not redesigned; generic loader takes a schema object as a parameter rather than importing a fixed module). ✅ **Flagged:** how Hugging Face's nested output maps onto a `ColumnDef` list at all is unresolved and called out as a likely point of friction, not silently assumed to work.
- **Adapter interface sketch:** shared-today list (session/retry/timeout/config/logging) is grounded in what `src/extract.py` actually does, not guessed. Four specific interface questions (page-fetch as a separate method, auth as a lifecycle step, response normalization's layer, pagination-strategy shape) are explicitly marked speculative pending a second and third real adapter. ✅
- **Sequencing:** confirmed as build-second-adapter-concretely → build-third-adapter-concretely → extract shared interface from the diff, explicitly mirrored against how `docs/schema_refactor_plan.md` did the same at the schema-file scope. ✅
- **Where I had to guess rather than verify:** (1) whether Twitch app registration requires a credit card, (2) every bullet under "guessed-at-being-shared" in the adapter interface section, (3) that a domain-matched (medical/IT-ops), free, card-free, `client_credentials` OAuth2 API exists at all — I did not find one in the time spent researching this doc, so absence-of-evidence is reported rather than treated as proof none exists.
