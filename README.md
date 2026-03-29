# @engine9/input-tools

Tools for reading, writing, and management of Engine9 style inputs.

The @engine9/input-tools are utilities for iterating through
records, appending statistics and other zip files. It's intended to be used by
third parties to interact with engine9 instances.

## Scope

This package is meant to stay **portable across environments** (CLI, workers, and
lightweight consumers). It should not depend on server-only storage layout,
optional analytics engines, or products that not every consumer installs. For
example, **DuckDB** is optional for callers of input-tools; timeline DuckDB file
paths and related defaults live in the Engine9 **server** (or your app) instead
of here.
