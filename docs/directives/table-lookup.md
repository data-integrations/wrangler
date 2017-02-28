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
field name `customerUserId` will be used as the lookup key.

```
  table-lookup customerUserId customerTable
```

Suppose that the following is data in the Table `customerTable`:

    +==========================================+
    | CustomerUserId    | City                 |
    +==========================================+
    | bobistheman       | Palo Alto, CA        |
    | joe1984           | Los Angeles, CA      |
    | randomUserqwerty  | New York City, NY    |
    +==========================================+

Also suppose that the input records to the directive are:

    +=================================================+
    | CustomerUserId    | Product      | Quantity     |
    +=================================================+
    | bobistheman       | Apples       |     10       |
    | joe1984           | Bicycle      |      1       |
    +=================================================+

Then the output Records of this directive will be:

    +=========================================================================+
    | CustomerUserId    | Product      | Quantity     | CustomerUserId_City   |
    +=========================================================================+
    | bobistheman       | Apples       |     10       |  Palo Alto, CA        |
    | joe1984           | Bicycle      |      1       |  Los Angeles, CA      |
    +=========================================================================+
