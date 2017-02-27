# Split to Columns

SPLIT-TO-COLUMNS directive splits a column based on separator into multiple columns.

## Syntax

```
 split-to-columns <column> <separator>
```

The ```column``` is split based on the ```separator```. ```separator``` can be defined as a regular expression.

## Usage Notes

The SPLIT-TO-COLUMNS directive takes the input column and applies the regex separator and generates as multiple
columns generated from the split. The name of the columns are in the format specified below:

```
  {
    "column" : "...",
    "column_1" : "...",
    "column_2" : "...",
    "column_3" : "...",
    ...
    "column_n" : "..."

  }
```

The original column when it was split to columns, it generated addition two columns for the record.
column_1 and column_2 ... column_n are the columns that includes 'n' parts of the split generated from applying
this directive.

So for example, if we have a ```separator``` pattern as "," over a string

```This will be split 1,This will be split 2,This will be split 3,Split 4```

will generate four new columns

```
  [1] This will be split 1
  [2] This will be split 2
  [3] This will be split 3
  [4] Split 4

```

> NOTE : This directive can only operate on string type columns.

## Example

Let's look at an example that will demonstrates the behavior of SPLIT-TO-COLUMNS directive. Let's start with a record

```
  {
    "id" : 1,
    "codes" : "USD|AUD|AMD|XCD",
  }
```

applying following SPLIT-TO-COLUMNS directive

```
  split-to-columns codes \\|
```
> NOTE: The backslashes are to escape the pipe(|) as it's a option separator in regex pattern.

would result in four (4) records being generated with each split value being assigned to the column ```codes```

```
  {
    "id" : 1,
    "codes" : "USD|AUD|AMD|XCD",
    "codes_1" : "USD",
    "codes_2" : "AUD",
    "codes_3" : "AMD",
    "codes_4" : "XCD"
  }
```

