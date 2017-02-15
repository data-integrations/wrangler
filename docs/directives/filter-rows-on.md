# Filter Row based on Regular Expression

FILTER-ROWS-ON directive provides a way to filters records from the dataset.

## Syntax
```
  filter-rows-on <filter-type> <options>
```

```filter-type``` specifies the type of filter and options associated with the filter specified.

```
  filter-rows-on condition <boolea-expression>
  filter-rows-on regex <regular-expression?
  filter-rows-on empty-or-null-columns <column>[,<column>]*
```

## Usage Notes

The FILTER-ROWS-ON directive applies the regular expression on a column value for every record.
 If regex matches the column value, then the record is omitted, else it's passed as-is to the input of the
 next directive in the pipeline.

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
  filter-rows-on condition country !~ US
```

Will result in filtering out records for individuals that are not in US.

Low let's say you want to filter out individual within US who's ```hrlywage``` is greater than 12.

```
  filter-row-on condition (country !~ US && hrlywage > 12)
```

Will result in records that have individual who reside in US and who's hourly wage is less than 12.
