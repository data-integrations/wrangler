# Diff Date

DIFF-DATE is a directive for taking the difference in two dates.

## Syntax

```
diff-date <column1> <column2> <destColumn>
```

## Usage Notes

The DIFF-DATE directive will take the difference between two Date objects, and put difference (in milliseconds)
into the destination column.

Note that this directive can only apply on two columns whose date strings have already been parsed, either using the
PARSE-AS-DATE directive or the PARSE-AS-SIMPLE-DATE.

## Examples

```
  diff-date laterDate earlierDate dateDiff
```
