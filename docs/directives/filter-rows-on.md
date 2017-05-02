# Filter Rows On

The `filter-rows-on` directive filters records based on a condition.


## Syntax
```
filter-rows-on <filter-type> <options>
```

The `<filter-type>` specifies the type of filter used and the options to be supplied to it.

Supported filter types and their options:

```
condition-false <boolean-expression>
condition-true <boolean-expression>
empty-or-null-columns <column>[,<column>]*
regex-match <regular-expression>
regex-not-match <regular-expression>
```


## Usage Notes

The `filter-rows-on` directive applies the boolean or regular expression on a column value
for each record. If expression matches or returns `true` for the column value, then the
record is omitted; otherwise, it is passed on as-is to the input of the next directive.


## Examples

Using this record as an example:
```
{
  "id": 1,
  "name": "Joltie, Root",
  "emailid": "jolti@hotmail.com",
  "hrlywage": 12.34,
  "gender": "Male",
  "country": "US"
}
```

Applying this directive:
```
filter-rows-on condition-true country !~ 'US'
```
would result in filtering out records for individuals that are not in the US (where
`country` does not match "US").

Applying this directive:
```
filter-rows-on condition-true (country !~ 'US' && hrlywage > 12)
```
would result in filtering out records for individuals that are not in the US (where
`country` does not match "US") and whose hourly wage (`hrlywage`) is greater than 12.
