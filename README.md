# NYC Yellow Taxi Trips – Distance Percentile Analysis

This repository contains a Python solution to extract NYC Yellow Taxi trips whose `trip_distance` is above a given percentile (default: 0.9), using the official NYC Yellow Taxi Parquet datasets.

The task is intentionally simple:

> *“Give me all the trips over the 0.9 percentile in distance travelled for any of the parquet files you can find there.”*

This implementation focuses on correctness, performance, and clarity, while being explicit about trade-offs and limitations.

---

## Approach

The solution follows a straightforward three-step process:

1. **Load a single Parquet file** (local or remote)
2. **Compute the distance percentile**
3. **Filter and return trips strictly above that threshold**

The design favors simplicity and determinism over premature optimization.

---

## Tooling Choices

### Python
Python was chosen for its strong data engineering ecosystem, readability, and ease of integration into larger pipelines or batch workflows.

### Polars
Polars is used as the DataFrame engine due to its performance and memory efficiency on large, columnar datasets.

In practice, this results in:
- Faster ingestion and filtering on multi-million-row Parquet files
- Lower memory usage compared to pandas
- A declarative, expression-based API that maps well to analytical transformations

For this workload, Polars provides a good balance between performance and implementation simplicity.

---

## Implementation Notes

### Ingestion
- Files are fully loaded into memory using Polars.
- Remote files (HTTP/HTTPS) are downloaded to a temporary location before being read.
- This approach works well for current NYC Yellow Taxi monthly files (≈2–4GB).

This is a conscious trade-off: full in-memory loading simplifies the implementation and performs well at this scale, but it is memory-bound by design.

### Percentile Calculation
- The percentile is computed on the full `trip_distance` column.
- Trips are filtered using a **strict comparison (`>`)**, matching the interpretation of “over the percentile”.
- Results are sorted by distance in descending order to make outputs deterministic and easier to inspect.

Empty inputs are handled gracefully and return an empty result.

### Output
- Writing results to disk is optional.
- If no output file is provided, a preview of the results is printed.
- When an output path is provided, results are written as a Parquet file and overwrite any existing file.

---

## Project Structure

The project is organized as a small package with a thin CLI wrapper:

- **Core logic** is implemented as pure functions, independent of I/O
- **CLI layer** handles argument parsing, file handling, and user interaction

This separation makes the core logic reusable and easy to test, while keeping the CLI simple.

---

## Requirements

- Python 3.9+
- Dependencies:
  - `polars`
  - `requests`

Install with:

```bash
pip install -r requirements.txt
```

---

## Usage


```bash
python yellow_taxi_percentile.py INPUT_FILE [OUTPUT_FILE] [--percentile P]
```

**Parameters**:
- `INPUT_FILE` (required): Local path or HTTP(S) URL to Parquet file
- `OUTPUT_FILE` (optional): Output Parquet file path. If omitted, prints preview
- `--percentile` (optional): Float between 0 and 1, default 0.9

**Examples**:

```bash
# Preview mode (no output file)
python yellow_taxi_percentile.py yellow_tripdata_2025-01.parquet

# Save results to file
python yellow_taxi_percentile.py yellow_tripdata_2025-01.parquet output.parquet

# Custom percentile from URL
python yellow_taxi_percentile.py https://example.com/data.parquet output.parquet --percentile 0.95
```

---

## Testing

I've included a simple test script (`yellow_taxi_percentile_test.py`) that validates core behavior without requiring external data files.

**What it tests**:
- Percentile calculation correctness
- Filtering logic (all returned trips are above threshold)
- Empty input handling
- Missing column error handling

Run it with:
```bash
python yellow_taxi_percentile_test.py
```

**Why not pytest?** For this scope, a lightweight test approach was preferred over a full testing framework. If this were part of a larger codebase, I'd use pytest with fixtures and parameterized tests. 

---

## Known Limitations & Trade-offs

- Assumes a standard NYC Yellow Taxi schema
- Processes one file at a time
- Memory-bound due to full in-memory loading
- No caching for repeated runs

**The philosophy**: These constraints are acceptable for the current dataset sizes and problem scope. If requirements changed (larger files, constrained memory, repeated queries), the implementation could be adapted to use chunked or approximate processing.

---

## Roadmap & Key Design Decisions

This project is designed to solve the stated problem efficiently while leaving room for future improvements.

Key design decisions include:

- **Polars over pandas** for performance and expressiveness  
- **Full in-memory loading** for simplicity given current file sizes  
- **Package structure** separating core logic from CLI for testability and reuse  
- **Strict `>` comparison** for percentile filtering  
- **Optional output file** to accommodate both exploratory and batch use cases  
- **Simple test script** matching problem complexity  

**Future enhancements** could include:

- Flexible column selection to support datasets with slight schema variations  
- Memory-efficient modes (chunked or streaming processing) for larger files  
- Optional output formats (e.g., CSV) for interoperability  
- Basic data quality checks (negative distances, missing timestamps)  

Decisions were made with trade-offs in mind, and the implementation is adaptable if requirements change or optimizations become necessary.

