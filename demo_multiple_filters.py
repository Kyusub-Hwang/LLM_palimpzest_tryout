#!/usr/bin/env python3
"""
Demonstration: Multiple sem_filter() calls with MomaDataset

This script shows how chaining multiple sem_filter() calls works.
Multiple filters are combined with AND logic during expansion.
"""

from ap_picker.datasets.moma.dataset import MomaDataset

# Load dataset
moma = MomaDataset(path='assets/moma_datasets/relational_db_item.json')

print("=" * 70)
print("Multiple sem_filter() Demonstration")
print("=" * 70)

# Example 1: Single filter
print("\n1. Single filter:")
single_filter = moma.sem_filter('algebra')
print(f"   Filters stored: {single_filter._lazy_filters}")
print(f"   Result: 1 filter condition")

# Example 2: Multiple chained filters
print("\n2. Multiple chained filters:")
multi_filter = (
    moma
    .sem_filter('questions about algebra')
    .sem_filter('basic difficulty level')
    .sem_filter('multiple choice format')
)
print(f"   Filters stored: {multi_filter._lazy_filters}")
print(f"   Result: {len(multi_filter._lazy_filters)} filter conditions")

# Example 3: How it works during expansion
print("\n3. How filters are applied during expand():")
print("   - Filters are combined with AND logic")
print("   - For SQL databases: WHERE condition1 AND condition2 AND condition3")
print("   - Only records matching ALL filters are returned")

# Example 4: SQL generation example
print("\n4. Example SQL generation:")
print("   Input filters:")
for i, f in enumerate(multi_filter._lazy_filters, 1):
    print(f"     {i}. {f}")
print("\n   Generated SQL (conceptual):")
print("     SELECT * FROM schema.questions")
print("     WHERE question ILIKE '%algebra%'")
print("       AND difficulty = 'basic'")
print("       AND format = 'multiple choice'")

# Example 5: Comparison with single filter
print("\n5. Filter restrictiveness:")
print("   Single filter:   Returns more records (less restrictive)")
print("   Multiple filters: Returns fewer records (more restrictive)")
print("   Each additional filter narrows down the results")

print("\n" + "=" * 70)
print("Key Takeaway:")
print("  moma.sem_filter(A).sem_filter(B).sem_filter(C)")
print("  -> Returns records matching A AND B AND C")
print("=" * 70)

# Example 6: Complete pipeline
print("\n6. Complete pipeline example:")
print("""
pipeline = (
    moma
    .sem_filter("questions about calculus")
    .sem_filter("advanced difficulty")
    .expand()  # Filters applied here via SQL WHERE
    .run(config=QueryProcessorConfig(...))
)
""")

print("✓ All filters are stored and applied efficiently at the database level!")
