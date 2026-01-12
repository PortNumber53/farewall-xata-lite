# farewall-xata-lite
Tool to migrate databases away from Xata lite to a regular PostgreSQL server.

## Features

- Migrates schema (tables, columns, primary keys)
- Handles Xata-specific types and defaults (e.g., converts `nextval` to `SERIAL`)
- Migrates data with progress bars
- Avoids `pg_dump` dependency

## Prerequisites

- [Go](https://go.dev/) 1.20 or later
- Access to Source (Xata) and Destination PostgreSQL databases

## Build

Build the tool from the project root:

```bash
go build -o migration-tool
```

## Configuration

Create a `.env` file in the same directory or set environment variables:

```bash
export XATA_DATABASE_URL="postgres://user:pass@host:port/dbname"
export DATABASE_URL="postgres://user:pass@host:port/dbname"
```

## Running the Migration

Run the binary:

```bash
./migration-tool
```

The tool will:
1.  Connect to both databases.
2.  Introspect the Source schema (tables, columns, primary keys).
3.  Create the schema on the Destination (dropping existing tables if any).
4.  Copy data table by table, showing a progress bar for each.

## Example Output

```text
Connecting to Source (Xata)...
Connected to Source.
Connecting to Destination (Postgres)...
Connected to Destination.
Introspecting schema...
Found 13 tables.
Creating schema on destination...
Schema created.
Starting data transfer...
Migrating table: users
  Copying 100% |████████████████████████████████████████| [1s:0s]
...
Migration completed successfully!
```
