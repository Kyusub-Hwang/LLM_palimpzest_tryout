# Multiple sem_filter() Support - Summary

## Problem
Previously, chaining multiple `sem_filter()` calls would **overwrite** previous filters:
```python
# Before fix
moma.sem_filter("algebra").sem_filter("calculus")
# Result: Only "calculus" filter was kept ❌
```

## Solution
Now multiple `sem_filter()` calls are **accumulated** and combined with **AND logic**:
```python
# After fix
moma.sem_filter("algebra").sem_filter("calculus").sem_filter("geometry")
# Result: ALL three filters are kept and combined ✓
# SQL: WHERE ... algebra ... AND ... calculus ... AND ... geometry ...
```

## Implementation Changes

### 1. Changed Filter Storage from Single String to List
```python
# Before
class MomaDataset:
    _lazy_filter: Optional[str]  # Single filter

# After  
class MomaDataset:
    _lazy_filters: List[str]  # Multiple filters
```

### 2. Updated sem_filter() to Append, Not Replace
```python
# Before
def sem_filter(self, filter_condition: str):
    return MomaDataset(items=self.items, lazy_filter=filter_condition)
    # Overwrites previous filter ❌

# After
def sem_filter(self, filter_condition: str):
    new_filters = self._lazy_filters + [filter_condition]
    return MomaDataset(items=self.items, lazy_filters=new_filters)
    # Appends to filter list ✓
```

### 3. Updated Expansion to Handle Multiple Filters
```python
# Before
def _expand_dataset_to_records(meta_record, filter_condition=None):
    if filter_condition:
        # Single WHERE condition
        
# After
def _expand_dataset_to_records(meta_record, filter_conditions=None):
    if filter_conditions and len(filter_conditions) > 0:
        # Combine with AND: WHERE cond1 AND cond2 AND cond3
        combined_filter = " AND ".join(filter_conditions)
```

## Usage Examples

### Example 1: Progressive Refinement
```python
# Start broad
base = moma.sem_filter("questions")

# Add more specific filters
math = base.sem_filter("about mathematics")

# Even more specific  
algebra = math.sem_filter("algebra topic")
basic = algebra.sem_filter("basic difficulty")

# All filters combined when expanding
records = basic.expand().run()
# SQL: WHERE ... questions ... AND ... mathematics ... AND ... algebra ... AND ... basic ...
```

### Example 2: Different Aspects
```python
# Filter by topic AND difficulty AND format
records = (
    moma
    .sem_filter("calculus derivatives")  # Topic
    .sem_filter("advanced level")        # Difficulty
    .sem_filter("multiple choice")       # Format
    .expand()
    .run()
)

# Result: Only records matching ALL three criteria
```

### Example 3: Validation
```python
from ap_picker.datasets.moma.dataset import MomaDataset

moma = MomaDataset(path='assets/moma_datasets/relational_db_item.json')

# Check filters are accumulated
f1 = moma.sem_filter('A')
print(f1._lazy_filters)  # ['A']

f2 = f1.sem_filter('B')
print(f2._lazy_filters)  # ['A', 'B']

f3 = f2.sem_filter('C')
print(f3._lazy_filters)  # ['A', 'B', 'C']

# Original unchanged (immutable pattern)
print(moma._lazy_filters)  # []
print(f1._lazy_filters)    # ['A']
```

## SQL Generation

### Input
```python
moma.sem_filter("algebra").sem_filter("basic").sem_filter("multiple choice")
```

### LLM Prompt
```
Generate SQL for records matching ALL conditions:
  - algebra
  - basic
  - multiple choice
  
Requirements: Use WHERE clause with AND to combine all conditions
```

### Generated SQL
```sql
SELECT * FROM schema.questions
WHERE question ILIKE '%algebra%'
  AND difficulty = 'basic'
  AND format = 'multiple_choice'
```

## Benefits

1. **Natural Chaining**: Intuitive to add filters progressively
2. **AND Logic**: Makes results more restrictive (as expected)
3. **Immutability**: Each call returns new instance, original unchanged
4. **Database Efficiency**: All filters combined in single SQL WHERE clause
5. **Flexibility**: Can build filter chains dynamically

## Testing

Added `test_multiple_sem_filters()` in [tests/test_int_sql_filter.py](tests/test_int_sql_filter.py):
```python
def test_multiple_sem_filters(sample_dataset: MomaDataset, models: TestModels):
    output = (
        sample_dataset
        .sem_filter("questions about algebra")
        .sem_filter("basic level")
        .expand()
        .run(...)
    )
    # Verifies both filters are applied with AND
```

## Files Modified

1. **ap_picker/datasets/moma/dataset.py**
   - Changed `_lazy_filter` → `_lazy_filters: List[str]`
   - Updated `sem_filter()` to append filters
   - Updated `expand()` and `_expand_dataset_to_records()` to handle list
   
2. **tests/test_int_sql_filter.py**
   - Added `test_multiple_sem_filters()` test
   - Updated documentation

3. **MOMA_DATASET_ARCHITECTURE.md**
   - Added multiple filter examples
   - Updated implementation details

## Complete Example

```python
from ap_picker.datasets.moma.dataset import MomaDataset
from palimpzest import QueryProcessorConfig

moma = MomaDataset(path='assets/moma_datasets/relational_db_item.json')

# Chain multiple filters
records = (
    moma
    .sem_filter("calculus")         # Topic filter
    .sem_filter("derivatives")      # Subtopic filter  
    .sem_filter("advanced")         # Difficulty filter
    .expand()                       # All filters applied via SQL WHERE
    .run(config=QueryProcessorConfig(available_models=[llama]))
)

# Result: Records matching calculus AND derivatives AND advanced
print(f"Found {len(records)} records matching all criteria")
```

✅ Multiple `sem_filter()` calls now work correctly with AND logic!
