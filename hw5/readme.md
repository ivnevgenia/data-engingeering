# Module 5 Homework: Data Platforms with Bruin

In this homework, we use Bruin to build a complete data pipeline, from ingestion to reporting.

---

## Question 1: Required Files/Directories

**Question:** In a Bruin project, what are the required files/directories?

**Options:**
1. `bruin.yml` and `assets/`
2. `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
3. `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`
4. `pipeline.yml` and `assets/` only

**Answer:** Option 3 - `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

**Explanation:**
- `.bruin.yml` must be in the root directory (where git is initialized)
- `pipeline.yml` can be in a `pipeline/` subdirectory or in the root
- `assets/` must be next to `pipeline.yml` (same directory level)

**Required Structure:**
```
project-root/
├── .bruin.yml          # Required: Environments + connections (in root)
└── pipeline/
    ├── pipeline.yml     # Required: Pipeline definition
    └── assets/          # Required: Asset files (next to pipeline.yml)
```

**Alternative flat structure** (also valid):
```
project-root/
├── .bruin.yml
├── pipeline.yml
└── assets/
```

---

## Question 2: Materialization Strategy

**Question:** You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which materialization strategy should you use for the staging layer that deduplicates and cleans the data?

**Options:**
- `append` - always add new rows
- `replace` - truncate and rebuild entirely
- `time_interval` - incremental based on a time column
- `view` - create a virtual table only

**Answer:** `time_interval`

**Explanation:**
- **Incremental processing**: Process specific date ranges (e.g., one month) without rebuilding the entire table
- **Re-processing capability**: Delete and re-insert data for a time period, allowing re-runs and deduplication
- **Matches data organization**: Data is organized by `pickup_datetime`, which aligns perfectly with the incremental key
- **Efficiency**: Only processes the specified time window, not all historical data

**Example:**
```sql
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp
```

**Why not others:**
- `append`: Would keep adding data without removing duplicates - re-processing the same month would create duplicate rows
- `replace`: Would truncate and rebuild the entire table - inefficient for large historical datasets
- `view`: Doesn't store data, so can't persist cleaned/deduplicated results

---

## Question 3: Pipeline Variables

**Question:** You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

**Options:**
- `bruin run --taxi-types yellow`
- `bruin run --var taxi_types=yellow`
- `bruin run --var 'taxi_types=["yellow"]'`
- `bruin run --set taxi_types=["yellow"]`

**Answer:** `bruin run --var 'taxi_types=["yellow"]'`

**Explanation:**
The `--var` flag accepts:
1. Key-value pairs: `--var key=value`
2. JSON strings: `--var '{"key": "value"}'` or `--var 'key=["array", "values"]'`

For array variables, you must pass a JSON array string. Since `taxi_types` is an array of strings, use:
```bash
bruin run --var 'taxi_types=["yellow"]'
```

**Why others don't work:**
- `--taxi-types yellow` - No such flag exists
- `--var taxi_types=yellow` - `yellow` is a string, not an array
- `--set taxi_types=["yellow"]` - No `--set` flag exists

**Documentation Reference:**
```bash
bruin run --var env=prod --var '{"users": ["alice", "bob"]}'
```

---

## Question 4: Running with Dependencies

**Question:** You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

**Options:**
- `bruin run ingestion.trips --all`
- `bruin run ingestion/trips.py --downstream`
- `bruin run pipeline/trips.py --recursive`
- `bruin run --select ingestion.trips+`

**Answer:** `bruin run ingestion/trips.py --downstream`

**Explanation:**
The `--downstream` flag runs the specified asset and all its downstream dependencies.

**Syntax:**
```bash
bruin run <path-to-asset-file> --downstream
```

**Example:**
```bash
bruin run ingestion/trips.py --downstream
```

This will:
1. Run `ingestion.trips`
2. Then run all downstream assets that depend on it (e.g., `staging.trips`, `reports.trips_report`)

**Why others don't work:**
- `--all` - No such flag exists; asset name format is incorrect
- `--recursive` - No such flag exists; wrong path
- `--select` - No such flag exists; `+` syntax doesn't exist

**Documentation Reference:**
```bash
bruin run assets/players.asset.yml --downstream
```

---

## Question 5: Quality Checks

**Question:** You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

**Options:**
- `unique: true`
- `not_null: true`
- `positive: true`
- `accepted_values: [not_null]`

**Answer:** `not_null: true`

**Explanation:**
The `not_null` check verifies that none of the values in the checked column are null.

**Correct YAML syntax:**
```yaml
columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
```

**Why others don't work:**
- `unique` - Checks for uniqueness, not NULL values
- `positive` - Checks that values are > 0 (for numeric columns)
- `accepted_values: [not_null]` - Checks that values are in a list; `not_null` isn't a value

**Documentation Reference:**
```yaml
columns:
  - name: one
    type: integer
    checks:
      - name: not_null
```

---

## Question 6: Lineage and Dependencies

**Question:** After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

**Options:**
- `bruin graph`
- `bruin dependencies`
- `bruin lineage`
- `bruin show`

**Answer:** `bruin lineage`

**Explanation:**
The `lineage` command helps you understand how a specific asset fits into your pipeline by showing:
- What the asset relies on (upstream dependencies)
- What relies on the asset (downstream dependencies)

**Usage:**
```bash
bruin lineage <path to the asset definition>
```

**Example:**
```bash
bruin lineage assets/players.asset.yml
bruin lineage ./pipeline/assets/ingestion/trips.py
```

**Flags available:**
- `--full` - Display all upstream and downstream dependencies, including indirect dependencies
- `--output`, `-o` - Specify output format (`plain` or `json`)

**Why others don't work:**
- `bruin graph` - No such command exists
- `bruin dependencies` - No such command exists
- `bruin show` - No such command exists

---

## Question 7: First-Time Run

**Question:** You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

**Options:**
- `--create`
- `--init`
- `--full-refresh`
- `--truncate`

**Answer:** `--full-refresh`

**Explanation:**
The `--full-refresh` flag truncates tables before running, ensuring they are created/replaced from scratch.

**Usage:**
```bash
bruin run --full-refresh pipeline.yml
```

**What it does:**
- Drops and recreates tables from scratch
- Ensures a clean state for first-time runs
- Useful when tables don't exist yet or you want to rebuild everything

**Example:**
```bash
bruin run ./pipeline/pipeline.yml --full-refresh --start-date 2022-01-01 --end-date 2022-01-31
```

**Why others don't work:**
- `--create` - No such flag exists
- `--init` - This is for initializing projects (`bruin init`), not for running pipelines
- `--truncate` - No such flag exists

**Documentation Reference:**
> Use `--full-refresh` to create/replace tables from scratch (helpful on a new DuckDB file).

---

## Summary

These questions cover the fundamental concepts of Bruin:
1. **Project structure** - Understanding required files and directories
2. **Materialization strategies** - Choosing the right strategy for your use case
3. **Pipeline variables** - Overriding variables at runtime
4. **Dependency management** - Running assets with downstream dependencies
5. **Quality checks** - Ensuring data quality with built-in checks
6. **Lineage visualization** - Understanding asset relationships
7. **First-time setup** - Creating tables from scratch

For more information, refer to the [Bruin Documentation](https://getbruin.com/docs/bruin/).
