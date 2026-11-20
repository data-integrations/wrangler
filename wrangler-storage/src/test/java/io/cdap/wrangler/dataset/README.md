# Wrangler Dataset Tests

This package contains tests for the storage-related functionality in the Wrangler framework,
particularly around recipe storage and pagination.

## RecipePageRequestTest.java

The `RecipePageRequestTest` class contains unit tests for the `RecipePageRequest` class, which is
responsible for handling recipe pagination. These tests verify:

1. **Range Calculation**: Ensures that scan ranges are properly calculated for different pagination scenarios.
2. **Validation**: Tests that the validation logic correctly handles:
   - Invalid page tokens
   - Invalid sort field names
   - Invalid sort orders
   - Invalid page sizes (zero or negative)

### Key Concepts

- **Page Request Builder**: The RecipePageRequest uses a builder pattern to configure pagination options.
- **Scan Range**: The range of keys to scan when retrieving a page of recipes.
- **Sort Options**: Recipes can be sorted by different fields like creation time or update time.
- **Page Token**: A token representing the point to resume pagination for subsequent requests.

### Sample Test

```java
@Test
public void testGetRangeForFirstPage() {
  NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
  RecipePageRequest pageRequest = RecipePageRequest.builder(namespace).build();
  Range range = pageRequest.getScanRange();
  Collection<Field<?>> fields = getNamespaceKeys(NAMESPACE_FIELD, GENERATION_COL, namespace);
  Assert.assertEquals(range, Range.create(fields, INCLUSIVE, fields, INCLUSIVE));
}
```

This sample test verifies that the scan range is correctly calculated for the first page of results
within a specific namespace.