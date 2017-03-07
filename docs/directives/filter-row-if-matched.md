# Filter Row based on Regular Expression

FILTER-ROW-IF-MATCHED directive provides a way to filters records that match the pattern for a column.

## Deprecated

Please use [FILTER-ROWS-ON](filter-rows-on.md) directive.

## Syntax
```
  filter-row-if-matched <column> <regex>
```

```regex``` is a valid regular expression that is evaluated on the column value for every record.

## Usage Notes

The FILTER-ROW-IF-MATCHED directive applies the regular expression on a column value for every record.
 If regex matches the column value, then the record is omitted, else it's passed as-is to the input of the
 next directive in the pipeline.

 If regex is 'null', the value is compared against all the 'null' as well JSON null values.

## Examples

Let's illustrate how this directive works with a concrete example.
Let's assume that you have input records as follows:

```
  {
    "id" : 1
    "name" : "Joltie, Root",
    "emailid" : "jolti@hotmail.com",
    "hrlywage" : "12.34",
    "gender" : "Male",
    "country" : "US",
  }
```

You want to operate only on the records where an individual is residing in ```country``` US.

```
  filter-row-if-true country !~ US
```

Will result in filtering out records for individuals that are not in US.

Low let's say you want to filter out individual within US who's ```hrlywage``` is greater than 12.

```
  filter-row-if-true (country !~ US && hrlywage > 12)
```

Will result in records that have individual who reside in US and who's hourly wage is less than 12.
