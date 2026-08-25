# Contributing to Argus

Thanks for considering a contribution. Argus is an Apache-2.0, multi-backend
ODBC driver; the bar we hold ourselves to is that **every advertised claim must
survive verification** — contributions are reviewed with that in mind.

## Building and testing

See `docs/BUILDING.md` for the dependency matrix. The short version (Linux):

```bash
sudo apt-get install cmake build-essential unixodbc-dev libglib2.0-dev \
    libjson-glib-dev libcurl4-openssl-dev libmariadb-dev libcmocka-dev
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
cd build && ctest -L unit --output-on-failure
```

Integration tests run against real engines via
`tests/integration/docker-compose.yml`.

## Ground rules

- **C11**, `-Wall -Wextra -Wpedantic` clean. Match the style of the file you
  touch.
- **No accept-and-ignore**: an attribute, option or API the driver cannot
  honor must return an error (`HYC00`/`HY092`), never a fake success. This is
  a hard rule — silent no-ops have been this driver's worst class of bug.
- **Dialect changes need evidence**: function-map or literal-style entries are
  added only after probing a live server (see the methodology notes in
  `src/odbc/dialect.c` and `docs/BI_TOOLS.md`).
- **Allocators**: do not mix libc `free()` with GLib-allocated memory or vice
  versa; keep each module on one allocator family.
- **Tests accompany fixes**: a bug fix comes with a regression test wherever
  the unit harness can reach the code.
- Backend additions should implement the vtable as completely as possible
  (`get_last_error`, `is_alive` with a real probe, `get_server_version`) —
  see `docs/ADDING_BACKENDS.md`.

## Commit and PR conventions

- One logical change per commit; imperative subject (`odbc: reject row-wise
  bind sizes the fetch path cannot honor`).
- Update `CHANGELOG.md` under `[Unreleased]` for user-visible changes.
- CI must be green (build matrix, unit tests, sanitizers, CodeQL).

## Governance

The project is stewarded by the Varga Foundation. The `obs_hooks` seam
(`include/argus/obs_hooks.h`) is a stable extension surface for out-of-tree
add-ons; changes to its signatures are breaking and need maintainer sign-off.

Security issues: see `SECURITY.md` — never as public issues.
