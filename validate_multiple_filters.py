#!/usr/bin/env python3
"""
Quick validation: Multiple sem_filter() support

This script validates that multiple sem_filter() calls work correctly.
"""

from ap_picker.datasets.moma.dataset import MomaDataset

print("Testing Multiple sem_filter() Support")
print("=" * 60)

# Load dataset
moma = MomaDataset(path='assets/moma_datasets/relational_db_item.json')

# Test 1: Initial state
print("\n✓ Test 1: Initial state")
print(f"  _lazy_filters: {moma._lazy_filters}")
assert moma._lazy_filters == [], "Should start with empty filter list"

# Test 2: Single filter
print("\n✓ Test 2: Single filter")
single = moma.sem_filter('algebra')
print(f"  _lazy_filters: {single._lazy_filters}")
assert single._lazy_filters == ['algebra'], "Should have one filter"

# Test 3: Two filters
print("\n✓ Test 3: Two chained filters")
double = moma.sem_filter('algebra').sem_filter('calculus')
print(f"  _lazy_filters: {double._lazy_filters}")
assert double._lazy_filters == ['algebra',
                                'calculus'], "Should have two filters"

# Test 4: Three filters
print("\n✓ Test 4: Three chained filters")
triple = moma.sem_filter('algebra').sem_filter(
    'calculus').sem_filter('geometry')
print(f"  _lazy_filters: {triple._lazy_filters}")
assert triple._lazy_filters == [
    'algebra', 'calculus', 'geometry'], "Should have three filters"

# Test 5: Verify original is unchanged
print("\n✓ Test 5: Original instance unchanged")
print(f"  Original _lazy_filters: {moma._lazy_filters}")
assert moma._lazy_filters == [], "Original should still be empty"

# Test 6: Filters are immutable (new instance each time)
print("\n✓ Test 6: Each call creates new instance")
f1 = moma.sem_filter('A')
f2 = f1.sem_filter('B')
print(f"  f1 filters: {f1._lazy_filters}")
print(f"  f2 filters: {f2._lazy_filters}")
assert f1._lazy_filters == ['A'], "f1 should only have 'A'"
assert f2._lazy_filters == ['A', 'B'], "f2 should have both"
assert f1 is not f2, "Should be different instances"

print("\n" + "=" * 60)
print("All validation tests passed! ✓")
print("\nMultiple sem_filter() support is working correctly:")
print("  - Filters stored as list in _lazy_filters")
print("  - Each call appends to the list (doesn't overwrite)")
print("  - Creates new instance (immutable pattern)")
print("  - Filters will be combined with AND during expand()")
print("=" * 60)
