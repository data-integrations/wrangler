# Table Lookup

TABLE-LOOKUP is a directive for performing lookups into Table datasets.

## Syntax

```
table-lookup <column> <table>
```

## Usage Notes

The TABLE-LOOKUP directive will use the given column as the lookup key into the specified Table dataset, for
each record. The column should be of type string. The values in the Row of the Table will be parsed as String
and placed in the record with new column names, constructed as the lookup key and the Row column value, joined
with an underscore.

## Example

The following example represents a lookup into the `customerTable` dataset of type Table, where the record's
field name `customerId` will be used as the lookup key.

```
  table-lookup customerId customerTable
```
