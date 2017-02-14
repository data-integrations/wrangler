# Extract Regex Groups

EXTRACT-REGEX-GROUP directive extracts the data from regex group into it's own column.
## Syntax

```
 extract-regex-group <column> <regex-with-groups>
```

The directive generates additional columns based on the regex groups. This ignores the $0 regex group.

## Usage Notes

If multiple groups are matched then it creates multiple columns. The base name of the columns is
appended with the group number the pattern is matched for.


## Example

Let's look at how this work with an example

```
  {
    "title" : "Toy Story (1995)"
  }
```

applying following directives

```
  extract-regex-group title [^(]+\(([0-9]{4})\).*
```

would result in record as follows

```
  {
    "title" : "Toy Story (1995)",
    "title_1_1 : "1995"
  }
```

```title_1_1``` follows the format of ```<column>_<match-count>_<match-position>```
