# Extract Regex Groups

EXTRACT-REGEX-GROUP directive extracts the data from regex group into it's own column.
## Syntax

```
 extract-regex-group <column> <regex-with-groups>
```

The directive perform in-place change of case.

## Usage Notes

If multiple groups
are matched then it creates multiple columns


## Example

Let's look at how this work with an example

```
  {
    "id" : 1,
    "gender" : "male",
    "fname" : "Root",
    "lname" : "JOLTIE"
    "address" : "67 MARS AVE, MARSCIty, Marsville, Mars"
  }
```

applying following directives

```
  uppercase gender
  titlecase lname
  lowercase address
```

would result in record as follows

```
  {
    "id" : 1,
    "gender" : "MALE",
    "fname" : "Root",
    "lname" : "Joltie"
    "address" : "67 mars ave, marscity, marsville, mars"
  }
```

