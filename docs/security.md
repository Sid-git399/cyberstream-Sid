# Security

## Application security (Section 48)
- **Input validation**: every event passes through
  `shared.schema.validate_event()` before it's trusted anywhere in the
  pipeline; anything that doesn't validate goes to the dead-letter queue
  with its error reason, never silently dropped or coerced into
  something plausible.
- **Request size limits**: `backend/app/main.py`'s
  `BodySizeLimitMiddleware` rejects request bodies over 2MB with a 413.
- **Parameterized queries**: all Postgres access goes through
  `psycopg` with parameter placeholders (`%s`), never string-formatted
  SQL — see every router in `backend/app/routers/`.
- **No command execution**: nothing in the codebase shells out to
  execute user- or event-supplied strings.
- **Environment-variable configuration**: brokers, DSNs, paths, and
  rates are all read from environment variables (`.env.example`); no
  secrets are committed.
- **CORS**: restricted to the configured frontend origin
  (`CORS_ORIGINS`), not `*`.

## Untrusted data
Synthetic event fields (usernames, hostnames, metadata) are treated as
untrusted input throughout — they flow into Parquet/Postgres via
parameterized writes and are never interpolated into shell commands,
SQL strings, or eval'd.

## No real attack execution (Section 49)
The generator (`generator/generator.py`) only ever constructs JSON
dictionaries representing telemetry. It does not open sockets to
arbitrary hosts, does not run exploit code, does not scan real networks,
and does not collect real credentials. "Suspicious process" events are
plain metadata (`{"suspicious": true, "process_name": "synthsvc.exe"}`)
— a label, not an executable.
