# Parse as Datetime

The PARSE-AS-DATETIME directive parses strings with datetime values to CDAP datetime type.


## Syntax
```
parse-as-datetime <column> "<pattern>"
```


## Usage Notes

The PARSE-AS-DATETIME directive will parse strings with datetime values to CDAP
datetime type. Pattern is the format of the `input` strings. 
The input values and pattern should have a date and time component.

If the column is `null` or is already a datetime field, applying this directive
is ano-op. The column to be parsed as a datetime should be of type string.


## Examples
See [FORMAT-DATE](format-date.md) for an explanation and examples of these pattern strings.
