# SchwabGateway code-quality review — 2026-10

Scope: `src/schwab_gateway/`, `packages/sdk/src/schwab_gateway_sdk/`,
`packages/token-store/src/schwab_token_store/`, `tests/`. Reviewed at `origin/main` 727cca5
(v0.7.1 merged). Phase 1 only: no code was changed.

## Baseline (recorded 2026-10-02, before any change)

| Check | Result |
|---|---|
| `uv run pytest -q` | **635 passed**, 2 warnings (websockets legacy deprecation; aiohttp `NotAppKeyWarning` from `auth.py:190` `PRINCIPAL_KEY` being a plain `str`) |
| `uv run ruff check .` | All checks passed |
| `uv run ruff format --check .` | **Fails: 49 files would be reformatted** (ruff 0.16.2; about 990 changed lines, mostly wrapped signatures that now fit in 100 columns) |

The Phase 2 rule "both must be as green after every change" cannot be met for the format check as
things stand. See decision **D0** below.

Frozen areas, beyond the contract list in the task:

- `config.py` belongs to a credential-proof archive pinned by SHA-256 on Helios. See the
  `GatewayUpstreamSettings` docstring and `tests/test_gateway_live_provider.py:755`. **No edits.**
- `credential_probe.py` and `probe_credentials.py` sit on the same probe path. They are treated as
  frozen because of the "if unsure, freeze" rule.
- `auth.py`, `admission.py` and `redaction.py` are frozen per hard constraint 2. Observations about
  them are listed but not proposed.

## 1. Executive summary: top 10 by value ÷ risk

| # | Change | Why it's worth it | Risk | LOC Δ |
|---|---|---|---|---|
| 1 | **Memoize the market calendar** (`upstream.py:173-231`) and hoist `session_end` out of the per-candle loop (`:1004`) | Measured: a 960-candle session-history miss takes **43.6 ms, of which ~32 ms is rebuilding the holiday set 4× per candle**, synchronously on the event loop. `functools.cache` on the year-keyed pure functions returns the same frozensets. | low | +3 |
| 2 | **One freshness helper** in `upstream.py`: `age = max(0, now-ts) if ts else None; stale = age is None or age > limit`, repeated 10× | Removes the most-repeated block in the largest module. Pure function, fully covered by existing tests. | low | −35 |
| 3 | **Shared test support**: `tests/support.py` with the single-principal authenticator (6 copies), READY readiness fake (5), empty quote upstream (4), token-file helpers (3), plus a `serving(app)` async context manager replacing ~60 `TestServer` start/try/finally/close blocks | Biggest LOC win with no assertion touched. | low | −270 |
| 4 | **SDK client: merge `_get_quotes` into `_get_typed`** (`client.py:147-218`). The two differ only in the noun inside each message, and every message keeps its exact text. Add `_required_symbol` / `_required_date` helpers. | 70 near-duplicate lines in the public client. The error classes and messages are unchanged. | low | −50 |
| 5 | **Evidence-file helpers**: one `sha256_file` (4 copies) and one `write_private_json` (exclusive create + fsync + chmod 0600, 6 copies) | Same durability semantics everywhere, and one place to audit them. | low | −35 |
| 6 | **API small dedupes**: readiness gate ×3 → `_not_ready_response()`, upstream error responses ×3 → named constants, full annotations (`_json`, `handler`, `build_response`, `_parse_index -> MoverIndex`, the response builders) | Clearer request path, and the response JSON is byte-identical. | low | −10 |
| 7 | **Dead-code sweep**: unreachable `try/except` around `Task.exception()` (3 copies), duplicate `OrderBookVenue` and `SYMBOL_PATTERN` definitions, stale runner docstring/comment | Removes code that can never run, plus definitions that could drift apart. | low | −15 |
| 8 | **Runner lifecycle**: three identical "background task for app lifetime" cleanup-ctx factories → one `_background_task_ctx` | Less boilerplate, and precise return types instead of `Any`. | low | −20 |
| 9 | **live_provider**: the candle-envelope parsing repeated 3× → `_candles(response)`. Type `_execute` generically. | Fewer copies of the error messages, and better typing at the provider boundary. | low | −16 |
| 10 | **Stream-capture dedupe**: identical `handle_book` closures, the venue subscribe `if/else` (3 copies) and the frame-service filters (3 copies) | Order-book capture and live feed share one implementation of each. | low | −33 |

Medium-risk items with large payoffs need your explicit go-ahead. They are listed in **section 4**:
removing the stale exclusive-capture path (−140), Annotated validators in the SDK models (−150),
reconnect-loop dedupe (−60), and token-store lock dedupe (−45).

## 2. Findings by module

Categories: **dead** = dead code, **dup** = duplication, **simp** = simplification, **perf** =
efficiency, **read** = readability, **struct** = module structure, **err** = error handling.
LOC Δ values are estimates.

### `upstream.py` (1,496 lines)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| U1 | 455-459, 509-517, 540-545, 613-626, 669-673, 804-813, 1013-1018, 1054-1059, 1247-1252, 1267-1271 | dup | The age/stale computation is copied 10 times. | `_freshness(event_ts, at, stale_after) -> tuple[float \| None, bool]` | low | −35 |
| U2 | 173-231, 961-966, 1004 | perf | `_market_holidays(year)` (Easter computation plus set build) runs 4× per `_regular_session_end` call, which runs once per candle. Measured at 32 of 43.6 ms for one extended-session miss, on the event loop. `_latest_completed_session` repeats it per day stepped. | `@functools.cache` on `_market_holidays` and `_early_close_sessions` (pure, year-keyed, return frozensets). Compute `session_end` once and pass it into `_is_regular_session`. Add a characterization test first that pins holidays and early closes for 2021–2030, including 2021-12-31 and 2027-12-31. | low | +3 |
| U3 | 582-660, 684-688 | simp | The option-chain loop nests 6 deep with an 80-line body, and the `crossed_markets_normalized` aggregate re-scans every contract's flags. | Extract `_normalize_contract(option, option_type, strike, …)` and track `any_crossed` inside the loop. Pin first: a characterization test that compares the full `model_dump()` of a mixed fixture chain (crossed, negative analytics, stale and missing timestamps). | low-med | −5 |
| U4 | 1453-1466 | dead | `try: completed.exception() except Exception: pass`. In a done-callback on a non-cancelled task, `exception()` cannot raise, so the `except` is unreachable. | `if not completed.cancelled(): completed.exception()` | low | −3 |
| U5 | 829-856 | simp | Four hand-rolled `x = a; if x is None: x = b` fallbacks. | `_first_number(item, "changePercent", "netPercentChange")` / `_first_integer` | low | −6 |
| U6 | 760-770, 869-878, 994-1008 | simp | Manual `dropped` counters. | History: `bars = [...]` then compare lengths. Movers/session: keep the counter only where the flag needs it. | low | −6 |
| U7 | 1395-1400 | read | `_fetch_option_chain(symbol, expiration, key)` takes `key`, which is derivable from the other two arguments. | Drop the parameter. | low | −2 |
| U8 | 781-801 | perf | The minute-window trim converts each bar to Eastern time twice and filters in a separate pass. | One pass that computes each bar's Eastern date once. Minor, because minute history is ≤5 days. | low | 0 |
| U9 | 1184-1189 | read | `getattr(provider, "get_spot_snapshot")` duck typing is invisible to `SpotPriceProvider`. | Add a `TimestampedSpotPriceProvider` Protocol so the optional method is documented. | low | +5 |
| U10 | whole file | struct | Three concerns share one file: calendar (~110 lines), normalizers (~600), adapters and caches (~650). | **Optional, after U1–U3:** `market_calendar.py` + `normalize.py`, with `upstream.py` re-exporting the `normalize_*` names. One test monkeypatches `schwab_gateway.upstream.MAX_OPTION_CHAIN_CONTRACTS_V1` (`test_gateway_option_chain.py:560`) and would need to retarget. Recommend the calendar split only; it is pure and cohesive. | med | 0 |
| U11 | 1416 | perf | **Left alone.** Every cache miss serializes the chain once just to size it (measured 9.7 ms per 2,000 contracts), and the API serializes it again. The size feeds the `gateway_option_chain_cache_bytes` gauge, so its semantics would change. A cache hit re-copies every contract to refresh freshness (10.6 ms per 2,000); this is inherent to per-request freshness. | none | — |

### `api.py` (1,039 lines)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| A1 | 516-524, 655-662, 673-681, 692-700, 764-773, 786-794, 805-814, 851-859, 887-896 | dup | Nine handlers open with the same preamble: capability check, parse, `ValueError` → 400. | **Needs approval (D3).** A `_market_data_handler` decorator that does the capability check, plus parsers raising a dedicated `_InvalidRequest(ValueError)` so the decorator never swallows a `ValueError` from the upstream or stream path. Order stays capability → validation → readiness. | med | −40 |
| A2 | 599-601, 739-741, 821-823 | dup | The readiness gate is copied 3 times. | `_not_ready_response(app) -> web.Response \| None` | low | −4 |
| A3 | 626-646, 753-760, 828-831 | dup | The same `_error("upstream_*", "market data upstream …", …)` triples appear in three places. | Module constants and a `_upstream_error(kind)` helper. | low | −6 |
| A4 | 193-243 | — | **Left alone.** There are six `_Unavailable*Upstream` classes. Each documents and type-checks one surface; a generic replacement would be clever rather than clearer. | none | — |
| A5 | 262, 403, 526+, 370, 714-716, 840-842, 196-232, 465 | read | Missing or loose annotations: `_json(model)`, `handler`, `build_response`, `_parse_index` (should return `MoverIndex`), `result` params, `_Unavailable*` returns. `ready(_request)` uses its "unused" parameter. | Full annotations; rename to `request`. | low | 0 |
| A6 | 398, 893 | read | The messages hard-code `1000` and `25` while the constants `MAX_RECENT_ORDER_BOOK_SNAPSHOTS` and `MAX_STREAM_ORDER_BOOK_SYMBOLS` exist. | f-strings using the constants (identical text). | low | 0 |
| A7 | 922-945 | perf | The WebSocket loop creates 2 tasks per delivered snapshot and cancels and recreates `socket.receive()` each time. | **Needs approval (D8).** Keep one long-lived receive task. Only matters at high snapshot rates; stream admission caps it at 4+2 streams. Measure before changing. | med | ±0 |
| A8 | 102, 384-388 | dup | Venue validation duplicates `order_book.BOOK_SERVICE_BY_VENUE` keys. | Derive `ORDER_BOOK_VENUES` from the shared `OrderBookVenue` (see O1). | low | −1 |
| A9 | whole file | struct | **Not recommended.** Splitting `api.py` would not clearly help navigation: it already reads top-down (keys → parsers → middleware → handlers → `create_app`). Revisit only if A1 is declined. | none | — |

### Capture modules (`order_book_capture.py` 681, `equity_stream_capture.py` 390, `capture_*.py`)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| C1 | order_book_capture.py:361-473 | dead | `capture_order_book_stream` and `run_exclusive_order_book_capture` (which holds the token lock for the whole stream) were superseded in 989dbd7 ("release token lock after stream bootstrap"). Nothing in production calls them; the first is exercised only by `test_gateway_order_book.py:359-387`. | **Needs approval (D1).** Delete both and the test that covers only them. Every other order-book test stays. | med | −140 |
| C2 | order_book_capture.py:546-625; equity_stream_capture.py:282-349 | dup | The bounded reconnect/backoff loop is copied almost line for line (~70 lines). | **Needs approval (D6).** `run_with_reconnects(manager, settings, client_factory, recorder, *, deadline, subscribe, …)`, where `subscribe(stream)` installs handlers. Event names, payloads and counters are unchanged. | med | −60 |
| C3 | order_book_capture.py:378-392, 563-577 | dup | The two `handle_book` closures are identical. | `_book_handler(decoder, recorder, venue)` (moot if C1 lands). | low | −15 |
| C4 | order_book_capture.py:402-407, 580-585; order_book_live.py:145-150 | dup | The NASDAQ/NYSE subscribe `if/else` appears 3 times. | `subscribe_venue_book(stream, venue, symbols, handler)` | low | −8 |
| C5 | order_book_capture.py:84-89, 318-323; equity_stream_capture.py:67-72, 222-227; export_session_history.py:21-22, 75-80, 97-102; order_book_analysis.py:25-30, 248-253, 276-281 | dup | 4× SHA-256 file hashing and 6× "exclusive-create JSON, flush, fsync, chmod 0600". | New `evidence_files.py` with `sha256_file` and `write_private_json` / `write_private_ndjson`. `issue_keys.write_private_json` is left as it is, because it opens with `O_EXCL` + `fchmod` and has different semantics. | low | −35 |
| C6 | order_book_capture.py:351-358; equity_stream_capture.py:243-250; order_book_live.py:31-45 | dup | Three copies of the "frame contains service X" filter. | `frame_has_service(payload, services: Container[str])` | low | −10 |
| C7 | order_book_capture.py:56-81; equity_stream_capture.py:43-64 | — | **Left alone.** The two request validators are near-identical, but almost every message differs. A shared helper would need one parameter per message for two call sites. | none | — |
| C8 | both recorders | dup | `start` / `record_connection_event` / `finalize` share their structure (~60 lines). | Optional after C2: a `_StreamEvidenceRecorder` base. The manifest and event payloads differ (`continuity_epoch`), so this is modest value. | med | −40 |
| C9 | order_book_capture.py:127-129, 362, 437; equity_stream_capture.py:98-100 | read | File handles typed `Any`; `client: Any`. | `BinaryIO \| None` / `TextIO \| None` | low | 0 |
| C10 | capture_order_books.py:57-58; capture_equity_streams.py:28-29; order_book_live.py:63, 67-68, 165 | read | Magic numbers that duplicate `DEFAULT_MAX_RECONNECTS`, `DEFAULT_STREAM_LOGIN_TIMEOUT_SECONDS`, `MAX_CAPTURE_SYMBOLS` and `MAX_RECONNECT_DELAY_SECONDS`. | Reference the constants (argparse defaults keep the same values). | low | 0 |
| C11 | capture_*.py `main` | — | **Left alone.** The two CLI `main()`s look alike, but sharing a helper across two sites with different events and fields would not read better. | none | — |

### `live_provider.py` (563)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| L1 | 335-342, 369-376, 430-437 | dup | The `raise_for_status` → JSON object → `candles` list check is copied 3 times. | `_candles(response) -> list[dict[str, Any]]` (same messages). | low | −16 |
| L2 | 170-176 | perf | The worker-lease wait polls every 1 ms. While a timed-out detached worker drains (≤ the 3 s HTTP timeout), each waiter wakes ~1,000×/s. | **Needs approval (D7).** Signal release through `loop.call_soon_threadsafe(event.set)`. Concurrency-sensitive; only hit during timeout drain. | med | +5 |
| L3 | 182 | read | `operation: Any`, returns `Any`. | `Callable[[Any], T] -> T` generic. | low | 0 |
| L4 | 204-210 | dead | The same unreachable `try/except` around `future.exception()` as U4. | Simplify. | low | −3 |
| L5 | 94-134 | — | **Left alone.** `extract_spot_price_and_timestamp` duplicates `upstream._event_time` on purpose: its docstring and a differential test pin bug-for-bug parity with the direct path. | none | — |

### `scheduler.py` (520)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| S1 | 330-367, 337-338, 366-367, 378-379, 519-520 | dup | `_remove_queued_locked` and `_expire_queued_locked` share their bookkeeping, and the `if not any(self._allocated.values()): self._idle.set()` idle check appears 4 times. | `_release_queued_locked(job)` + `_set_idle_if_drained()` | low | −10 |
| S2 | 413-520 | simp | `_run_job` is 108 lines long. | **Left alone for now.** The non-preemptible drain semantics are subtle, and the payoff is readability only. Revisit with a dedicated PR if you want it. | med | −0 |
| S3 | 119-125 | dead | The `except (Exception, CancelledError)` can never fire, because the guard above already checks `cancelled()`. | Simplify (same as U4). | low | −3 |
| S4 | 241, 252 | read | `asyncio.get_running_loop().time()` is called while a `loop` variable is already in scope. | Use `loop`. | low | 0 |

### `runner.py` (324)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| R1 | 217-276 | dup | Three factories have the same create-task / yield / cancel / suppress shape and return `Any`. | `_background_task_ctx(start: Callable[[], Coroutine[Any, Any, None]])`. The warmup ctx keeps its bounded startup attempt. | low | −20 |
| R2 | 98-110, 167-187 | dup | The `AdmissionPolicy` and `OrderBookSnapshotStore` construction from settings is duplicated across the demo and live apps. | `_order_book_kwargs(settings)` | low | −8 |
| R3 | 119, 152-154 | read | The docstring says "three read surfaces" (there are seven), and the comment at 152 repeats the docstring at 128-130. | Fix the docstring and drop the duplicate comment. | low | −3 |
| R4 | 117 | read | `client_factory: Any` | `SchwabAccessFunctionClientFactory[Any]` | low | 0 |

### Order book (`order_book.py`, `order_book_store.py`, `order_book_live.py`, `order_book_analysis.py`, `order_book_catalog.py`)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| O1 | order_book_store.py:14; order_book.py:22 | dup | `OrderBookVenue` is defined twice. | The store imports it from `order_book`. | low | −1 |
| O2 | order_book_store.py:78-84 | perf | **Left alone.** `tuple(deque)[-limit:]` copies the whole history, but measured 49 µs at 10k entries, which is immaterial. | none | — |
| O3 | order_book_live.py:162-174 | — | **Not redundant (checked).** `mark_feed_state("disconnected")` in `except` *and* in `finally` looks duplicated, but the `except` copy covers the backoff sleep. Keep it. | none | — |
| O4 | order_book_analysis.py:33-42; order_book_catalog.py:17-23 | dup | The relative-evidence-path escape check is written twice. | One `_relative_evidence_path(manifest, value) -> Path \| None` helper; the analysis module raises on `None`. | low | −6 |
| O5 | order_book_analysis.py:89-229, 151-160 | simp | `derive_metrics` is 140 lines, and the add/removal rate expressions are duplicated. | Extract `_snapshot_row(...)`. Offline tool; optional. | low | −5 |
| O6 | order_book_analysis.py:237; export_session_history.py:31 | read | `clock: Any` | `Callable[[], dt.datetime] \| None` | low | 0 |
| O7 | order_book_catalog.py:141-145 | simp | `try: unlink except FileNotFoundError: pass` | `contextlib.suppress` | low | −2 |

### Smaller modules

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| M0 | api.py:70; order_book_capture.py:35; export_session_history.py:18 | dup | `SYMBOL_PATTERN` is defined 3 times in non-frozen gateway modules; it is also in `config.py` (frozen) and in the SDK (boundary). | Import a single gateway definition. | low | −2 |
| T1 | ttl_analysis.py:111 | read | `--current-ttl-seconds` default `4.0` duplicates `DEFAULT_OPTION_CHAIN_CACHE_TTL_SECONDS`. | Reference the constant. | low | 0 |
| LT1 | load_test.py:221-226 | read | The allowlist set is rebuilt for every row. | Module-level `frozenset` constant. | low | 0 |
| LT2 | load_test.py:79, 95-98, 208 | read | `32` and `20` are hard-coded in messages; `_handle: Any`. | Use the constants; type it `TextIO \| None`. | low | 0 |
| LG1 | logging.py:28, 33, 40 | read | `getattr(logging, log_level.upper())` is computed twice; `get_logger` has no return type. | Compute once and annotate. | low | −1 |

### Frozen modules: observations only, no change proposed

| ID | file:line | Observation |
|---|---|---|
| F1 | auth.py:190 | `PRINCIPAL_KEY` is a `str`, which triggers aiohttp's `NotAppKeyWarning` in the test run. `web.RequestKey` would silence it with no behavior change, but `auth.py` is frozen (**D5**). |
| F2 | auth.py:20-26, 36-40 | `KNOWN_CLIENT_IDS` and `LEGACY_PRIORITY_BY_CLIENT` are unreferenced. The comment says they are kept on purpose as documentation. |
| F3 | redaction.py:29 | `redact()` has no production caller; only `tests/test_gateway_redaction.py` uses it. Flagged, not proposed. |
| F4 | issue_keys.py:27, 146-147 | `key_digest` duplicates `auth.hash_api_key`, minus the empty-key check. The plaintext cleanup branch is unreachable because nothing can raise after `plaintext_written = True`. This is key-issuance code, so it is left as it is. |
| F5 | config.py | Its validators hard-code 8, 16, 25, 1000 and 10,000, which duplicate constants elsewhere. The file is SHA-pinned, so it must not be edited. |

### SDK (`packages/sdk`)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| K1 | client.py:147-218 | dup | `_get_quotes` and `_get_typed` are identical apart from "quote" vs "market data" in five messages. | One `_get_typed(path, params, model, *, noun="market data")`. Every message keeps its exact text. | low | −35 |
| K2 | client.py:69-77, 102-109 | dup | Two copies of the error-code extraction. | `_error_code_from(payload: object)` | low | −5 |
| K3 | client.py:221-286 | dup | `symbol.strip()` + "a symbol is required" appears 6 times, and the date-not-datetime check 3 times. | `_required_symbol`, `_required_date(value, message)` | low | −12 |
| K4 | client.py:147, 186 | read | `model: type`, so the private helpers return an untyped value. | `type[ModelT] -> ModelT`. Public signatures are unchanged. | low | 0 |
| K5 | client.py:302-306 | dup | Re-implements `_normalize_order_book_venue` with the same message. | Call the helper. | low | −3 |
| K6 | models.py:38-52, 93-107, 131-145, 264-280, 343-357, 389-394, 415-429, 457-471, 520-534, 619-626 | dup | ~16 copies of the timezone-aware and non-negative-age validators. | **Needs approval (D2).** `Annotated[...]` types with `AfterValidator`: `GatewayTimestamp`, `NonNegativeAge`, `FiniteNonNegativeAge`, each keeping its exact message. The order-book variant keeps its own message. This removes the `*_must_be_*` classmethods from the model classes. Nobody should call them, but they are technically SDK surface, so this is flagged rather than assumed. | med | −150 |
| K7 | models.py:696-706 | — | **Left alone.** The `GatewayReadinessV1` Literals mirror `TokenManagerState`, but importing token-store into the SDK would add a dependency. | none | — |

### Token store (`packages/token-store`, 756 lines)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| TS1 | 289-427 | dup | `read_locked` and `locked` share ~55 lines: thread-lock acquire, flock retry loop, metrics, release. | **Needs approval (D6).** A private `_hold_lock(mode, open_fd, flock_op)` context manager. Metric labels, timeouts and error messages are identical. | med | −45 |
| TS2 | 638-672 | dup | `_record_load_failure` and `_record_refresh_failure` duplicate the exception → (state, reason) mapping. | One table `(exc_type, state, reason, refresh_result)`; the refresh path adds the counter. | low | −15 |
| TS3 | 187, 204, 314-318, 385-390 | read | `getattr(os, "O_CLOEXEC", 0)` and `O_NOFOLLOW` are repeated. | Module constants. | low | −2 |
| TS4 | whole file | struct | **Do not split.** Tests patch `schwab_token_store.log` (`test_gateway_token_manager.py:288, 317, 359`), and a single-file package keeps the boundary obvious. After TS1 and TS2 the file is about 690 lines. | none | — |
| TS5 | 82 | — | **Left alone.** `TokenManagerState(str, Enum)` → `StrEnum` would change what `str()` returns, so it would change behavior. | none | — |

### Tests (11.6k lines, no `conftest.py`)

| ID | file:line | Cat | What's wrong | Proposed change | Risk | LOC Δ |
|---|---|---|---|---|---|---|
| X1 | test_gateway_api.py:59, test_gateway_collector_surfaces.py:54, test_gateway_order_book_api.py:31, test_gateway_option_chain.py:1093, test_gateway_session_history_cache.py:251, test_gateway_admission.py:39 (variant); readiness fakes ×5; empty quote upstreams ×4; `_BlockingSpotUpstream` ×2; `_one_slot_app` ×2; `token_document`/`write_token` ×3; `_Clock` ×2 | dup | The same fixtures are re-declared in each file. | `tests/support.py` with plain functions and classes (no fixture magic), imported explicitly. | low | −150 |
| X2 | ~60 sites across 8 files | dup | The same `server = TestServer(app); await server.start_server(); try: … finally: await server.close()` block appears ~60 times, usually followed by SDK client create/close. | `async with serving(app) as base_url:` in `tests/support.py` | low | −120 |
| X3 | 156 sites | dead | `@pytest.mark.asyncio` is a no-op under `asyncio_mode = "auto"`. | Optional: remove. Pure noise reduction, no assertion touched. | low | −156 |

## 3. Proposed PR sequence

Each PR covers one theme, is cut from `main` as `refactor/<theme>`, and should take under 15
minutes to review. Each one records pytest/ruff before and after, plus its net LOC.

| PR | Branch | Contents | Characterization test first? |
|---|---|---|---|
| 0 | `style/ruff-format` | **(D0)** Mechanical `ruff format .` only. Reviewers can verify it by re-running ruff. This restores a green `--check` baseline. | n/a |
| 1 | `refactor/market-calendar-cache` | U2 | Yes: holidays and early closes 2021–2030, plus the session split around early closes |
| 2 | `refactor/upstream-freshness` | U1, U7 | Existing coverage is sufficient |
| 3 | `refactor/upstream-normalizers` | U3, U5, U6, U8, U9 | Yes: full `model_dump()` of a mixed option-chain fixture |
| 4 | `refactor/dead-code` | U4, L4, S3, O1, M0, R3, A8 | — |
| 5 | `refactor/named-constants` | A6, C10, T1, LT1, LT2, TS3, LG1 | — |
| 6 | `refactor/api-handlers` | A2, A3, A5 | Golden contract tests already pin the bodies |
| 7 | `refactor/sdk-client` | K1–K5 | Yes: one test per status → exception class + message for quotes and typed reads |
| 8 | `refactor/evidence-files` | C5, O4, O6, O7 | Yes: manifest bytes, file modes and hashes for one capture and one export |
| 9 | `refactor/stream-capture-helpers` | C3, C4, C6, C9 | — |
| 10 | `refactor/live-provider` | L1, L3 | — |
| 11 | `refactor/runner-lifecycle` | R1, R2, R4 | — |
| 12 | `refactor/scheduler-bookkeeping` | S1, S4 | — |
| 13 | `refactor/token-store-failures` | TS2 | Yes: state, reason and refresh-counter table for every failure class |
| 14 | `test/shared-support` | X1, X2 (X3 if approved) | n/a (assertions untouched; test count must stay 635+) |

Estimated net LOC, PRs 1–14 without the decision items: roughly **−270 in src/packages and −270 in
tests**. PR 0 is separate; it reflows ~990 lines with a net reduction.

## 4. Decisions needed before Phase 2

| ID | Question | Recommendation |
|---|---|---|
| D0 | `ruff format --check` is red on `main` (49 files). Should a format-only PR land first, or should the gate be "no new format drift in touched files"? | Land PR 0 first; otherwise every refactor PR mixes reformatting with real changes. |
| D1 | Delete the superseded exclusive-lock capture path (C1) and its single dedicated test? | Yes. It is dead since 989dbd7, and keeping it invites someone to reuse the lock-for-the-whole-stream pattern that commit removed. |
| D2 | Replace the SDK models' repeated validators with `Annotated` types (K6)? This removes classmethods from public model classes. | Yes, if you agree those methods aren't API. The JSON schema, error messages and error locations are unchanged; a characterization test will pin the error text. |
| D3 | API handler decorator (A1)? | Optional. A2, A3 and A5 capture most of the value at lower risk. |
| D5 | Switch `auth.PRINCIPAL_KEY` to `web.RequestKey` to fix the warning? | Your call. Behavior is identical, but `auth.py` is frozen. |
| D6 | Dedupe the capture reconnect loop (C2) and the token-store lock acquisition (TS1)? | Yes for both, each in its own PR after its low-risk siblings. |
| D7 | Replace the live-provider 1 ms lease polling with an event (L2)? | Defer. It only matters during timeout drain; revisit if the load test shows lag spikes. |
| D8 | Keep a persistent receive task in the order-book WebSocket loop (A7)? | Defer until the load test exercises streams. |
| U10 | Extract `market_calendar.py` from `upstream.py`? | Yes, after PRs 1–3. The fuller `normalize.py` split is optional. |

## Deliberately left alone (summary)

- `config.py`, the credential-probe modules, `auth.py`, `admission.py` and `redaction.py`: frozen.
- The parity duplication in `extract_spot_price_and_timestamp` is pinned by design.
- The `_Unavailable*Upstream` classes and the two capture request validators: shared helpers would
  read worse than the current code.
- The option-chain size serialization and the per-hit contract copies: gauge semantics and
  freshness are inherent to them (measured and documented in U11).
- `order_book_store.recent` copying the deque: measured as immaterial.
- Splitting `api.py` or the token-store package: navigation gain doesn't justify the churn.
- The `UTC = dt.timezone.utc` aliases across ~12 modules: churn without benefit.
- Environment note: the project `.venv` runs Python 3.13 while `requires-python` and the ruff target
  are 3.12. CI should run the gates on 3.12 too.
