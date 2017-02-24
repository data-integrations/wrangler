# Simple Date Parser

PARSE-AS-SIMPLE-DATE is a directive for simple date strings.

## Syntax

```
parse-as-simple-date <column> <pattern>
```

## Usage Notes

The PARSE-AS-SIMPLE-DATE directive will parse the given date string, using the given pattern string, leveraging
Java's java.text.SimpleDateFormat class.

## Examples

```
  parse-as-simple-date entryTime MM/dd/yyyy HH:mm
  parse-as-simple-date birthdate yyyy.MM.dd
```
