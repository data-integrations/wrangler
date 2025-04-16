# Format Datetime

The FORMAT-DATETIME directive formats CDAP datetime values to custom pattern strings.


## Syntax
```
format-datetime <datetime_column> "<pattern>"
```


## Usage Notes

The FORMAT-DATETIME directive will format CDAP datetime values to custom pattern strings.
Pattern is the format for the output string. 


If the column is `null` applying this directive is a no-op. 
The column to be formatted should be of type datetime.


## Examples
See [FORMAT-DATE](format-date.md) for an explanation and examples the pattern strings.
