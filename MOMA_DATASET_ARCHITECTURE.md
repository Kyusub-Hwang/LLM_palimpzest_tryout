# MomaDataset Three-Method Architecture

## Overview

MomaDataset now implements a clean three-method architecture that separates:
1. **Dataset discovery** (metadata-level filtering)
2. **Data filtering** (content-level, lazy evaluation)
3. **Schema transformation** (dataset-level → row-level)

This follows Palimpzest's philosophy of lazy evaluation and integrates seamlessly with the pipeline.

---

## The Three Methods

### 1. `find(filter_condition, depends_on=None)` → Dataset

**Purpose**: Discover datasets based on metadata only (description, type, etc.)

**Behavior**:
- Filters at the **dataset level** without accessing actual data
- Uses `DatasetLevelFilter` operator:
  - For relational DBs: SQL `LIMIT 1` existence check
  - For file datasets: LLM evaluation on metadata
- Returns a Palimpzest `Dataset` with filtered meta-records

**When to use**: "Which datasets are relevant to my task?"

**Example**:
```python
# Find datasets about mathematics
math_datasets = (
    moma.find("datasets about mathematics", depends_on=["description"])
    .run(config=QueryProcessorConfig(available_models=[llama]))
)
```

**Schema returned**: Original `moma_dataset_schema` (id, description, type, content, metadata)

---

### 2. `sem_filter(filter_condition)` → MomaDataset

**Purpose**: Lazily specify a filter for dataset content

**Behavior**:
- **Does NOT execute immediately** - stores filter condition in `_lazy_filters` list
- Returns a new `MomaDataset` instance with filter added to list
- **Multiple filters can be chained** - combined with AND logic:
  ```python
  moma.sem_filter("algebra").sem_filter("basic")
  # -> Filters for records matching BOTH "algebra" AND "basic"
  ```
- Filter is applied during `expand()` call:
  - For relational DBs: Generates SQL `WHERE` clause with AND conditions
  - For file datasets: Could filter during read (not yet implemented)

**When to use**: "Filter the actual data inside datasets"

**Example**:
```python
# Single filter
filtered_moma = moma.sem_filter("questions about algebra")

# Multiple filters (combined with AND)
multi_filtered = (
    moma
    .sem_filter("questions about algebra")
    .sem_filter("basic difficulty level")
    .sem_filter("multiple choice format")
)

# Filters applied when expand() is called
records = multi_filtered.expand().run(...)
```

**Schema returned**: Same `MomaDataset` instance (filters stored internally)

**Filter Combination Logic**:
- Single filter: `WHERE condition1`
- Multiple filters: `WHERE condition1 AND condition2 AND condition3`
- Each additional filter makes results more restrictive

---

### 3. `expand()` → Dataset

**Purpose**: Convert dataset-level to row-level (materialize individual records)

**Behavior**:
- Reads actual data from datasets (DB tables, files, etc.)
- Applies any lazy filters from `sem_filter()`:
  - For relational DBs: Uses LLM to generate SQL with `WHERE` clause
  - Executes filtered query for efficiency
- Wraps each record in envelope schema with source information
- Returns Palimpzest `Dataset` with row-level records

**When to use**: "Give me individual records (with optional filters applied)"

**Example**:
```python
# Without filter - expand all
all_records = moma.expand().run(...)

# With lazy filter - applies SQL WHERE during expansion
filtered_records = (
    moma.sem_filter("algebra questions")
    .expand()  # Filter applied here via SQL WHERE
    .run(...)
)
```

**Schema returned**: Envelope schema (source_dataset_id, source_dataset_desc, source_dataset_type, record_data)

---

## Pipeline Examples

### A. Metadata Discovery Only
```python
# Find relevant datasets by metadata
datasets = (
    moma
    .find("datasets about mathematics")
    .run(config=QueryProcessorConfig(available_models=[llama]))
)

# Result: Meta-records with fields (id, description, type, content, metadata)
```

### B. Lazy Data Filtering (Most Efficient)
```python
# Single lazy filter + expand
records = (
    moma
    .sem_filter("questions about algebra")  # Filter stored (not executed)
    .expand()  # Filter applied via SQL WHERE during expansion
    .run(config=QueryProcessorConfig(available_models=[llama]))
)

# Result: Row-level records with envelope schema
# SQL executed: SELECT * FROM schema.questions WHERE question ILIKE '%algebra%'
```

### B2. Multiple Lazy Filters (Combined with AND)
```python
# Multiple chained filters
records = (
    moma
    .sem_filter("questions about algebra")  # First filter
    .sem_filter("basic difficulty level")   # AND second filter
    .sem_filter("multiple choice format")   # AND third filter
    .expand()  # All filters applied via SQL WHERE ... AND ... AND ...
    .run(config=QueryProcessorConfig(available_models=[llama]))
)

# Result: Row-level records matching ALL filter conditions
# SQL executed: 
#   SELECT * FROM schema.questions 
#   WHERE question ILIKE '%algebra%'
#     AND difficulty = 'basic'
#     AND format = 'multiple choice'
```

### C. Expand Without Filtering
```python
# Get all records from all datasets
all_records = (
    moma
    .expand()  # No filter, reads all data
    .run(config=QueryProcessorConfig(available_models=[llama]))
)

# Result: All records from all datasets
```

### D. Post-Expansion Filtering (Less Efficient)
```python
# Expand first, then filter (uses RecordLevelFilter)
records = (
    moma
    .expand()  # Materializes all records
    .sem_filter("algebra")  # LLM per-record evaluation
    .run(config=QueryProcessorConfig(available_models=[llama]))
)

# Less efficient: Loads all records, then filters with LLM
# Better: Use sem_filter() BEFORE expand() for SQL WHERE clause
```

---

## Operator Integration

The three methods integrate with Palimpzest's operator system:

| Method | Palimpzest Operator | Return Type | Executes? |
|--------|-------------------|-------------|-----------|
| `find()` | `DatasetLevelFilter` | `Dataset` | Via `.run()` |
| `sem_filter()` | None (state stored) | `MomaDataset` | No (lazy) |
| `expand()` | `flat_map` + filter application | `Dataset` | Via `.run()` |

---

## Architecture Benefits

1. **Separation of Concerns**:
   - `find()`: Metadata-level dataset discovery
   - `sem_filter()`: Content-level data filtering
   - `expand()`: Schema transformation (dataset → row-level)

2. **Lazy Evaluation**:
   - `sem_filter()` stores filter without executing
   - Filter applied during `expand()` via SQL WHERE
   - Follows Palimpzest's lazy evaluation philosophy

3. **Efficiency**:
   - `find()`: Uses SQL LIMIT 1 for existence checks (doesn't load data)
   - `sem_filter() + expand()`: Uses SQL WHERE clause (filters at DB level)
   - Avoids loading unnecessary data into memory

4. **Clarity**:
   - Method names clearly indicate their purpose
   - Return types guide pipeline construction
   - Easy to understand what executes when

---

## Implementation Details

### Lazy Filter Storage (Multiple Filters Support)
```python
class MomaDataset(IterDataset):
    _lazy_filters: List[str]  # Stores multiple filter conditions
    
    def sem_filter(self, filter_condition: str):
        # Appends new filter to the list (doesn't overwrite)
        new_filters = self._lazy_filters + [filter_condition]
        
        return MomaDataset(
            items=self.items,
            lazy_filters=new_filters
        )
```

**Key Feature**: Each `sem_filter()` call creates a new instance with an **extended** filter list, not a replaced filter.

**Chaining Behavior**:
```python
moma._lazy_filters             # []
moma.sem_filter("A")._lazy_filters          # ["A"]
moma.sem_filter("A").sem_filter("B")._lazy_filters    # ["A", "B"]
moma.sem_filter("A").sem_filter("B").sem_filter("C")._lazy_filters  # ["A", "B", "C"]
```

### Filter Application During Expansion
```python
def expand(self):
    lazy_filters = self._lazy_filters  # Capture the filter list
    
    return self.flat_map(
        udf=lambda meta: self._expand_dataset_to_records(meta, lazy_filters),
        cols=envelope_schema
    )

@staticmethod
def _expand_dataset_to_records(meta_record, filter_conditions=None):
    if filter_conditions and len(filter_conditions) > 0:
        # Combine multiple filters into a single prompt for LLM
        combined_context = " AND ".join(filter_conditions)
        
        # LLM generates SQL: WHERE cond1 AND cond2 AND cond3
        sql = generate_filtered_query(schema, filter_conditions)
        records = reader.read_stream(query=sql)
    else:
        records = reader.read_stream()
```

**SQL Generation with Multiple Filters**:
```
Input filters: ["algebra", "basic level", "multiple choice"]

LLM Prompt:
  "Generate SQL for records matching ALL conditions:
   - algebra
   - basic level  
   - multiple choice"

Generated SQL:
  SELECT * FROM questions
  WHERE question ILIKE '%algebra%'
    AND difficulty = 'basic'
    AND format = 'multiple_choice'
```

---

## Testing

All tests pass successfully:

```bash
$ uv run pytest tests/test_int_sql_filter.py -v

tests/test_int_sql_filter.py::test_int_sql_filter PASSED              [17%]
tests/test_int_sql_filter.py::test_two_level_filtering PASSED         [33%]
tests/test_int_sql_filter.py::test_dataset_level_filter_only PASSED   [50%]
tests/test_int_sql_filter.py::test_record_level_filter_only PASSED    [67%]
tests/test_int_sql_filter.py::test_complete_pipeline PASSED           [83%]
tests/test_int_sql_filter.py::test_multiple_sem_filters PASSED        [100%]

========================= 6 passed =========================
```

Each test demonstrates a different aspect of the architecture:
- `test_int_sql_filter`: find() for metadata filtering
- `test_two_level_filtering`: sem_filter() + expand() with lazy filter
- `test_dataset_level_filter_only`: find() metadata discovery
- `test_record_level_filter_only`: expand() without filtering
- `test_complete_pipeline`: Complete workflow demonstration
- `test_multiple_sem_filters`: **NEW** - Chaining multiple sem_filter() calls (combined with AND)

---

## Future Enhancements

1. **File Dataset Filtering**: Implement lazy filtering for file datasets
2. **Configurable Model**: Allow model selection for SQL query generation
3. **Filter Caching**: Cache generated SQL queries for repeated filters
4. **Combined find() + sem_filter()**: Support chaining (currently blocked by type mismatch)
