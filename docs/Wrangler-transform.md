# Wrangler Transform

A plugin for performing data transformation based on directives. The directives are generated either by an interactive user interface or manual entered into the plugin.

## Directives
Wrangler plugin supports an easy way to specify data transformation using directives. Directives are
instructions that tell plugin how to transform the incoming record. All of the directives are transformational
and they operate on the input row to generate a new row. The directives are applied on the input record in
the order they are specified.

## Types of Directives
Following are different types of directives that are supported by the Wrangler plugin.

### Parser

This directive specifies how the input needs to be parsed. Currently Wrangler supports parsing of CSV feed.
The input is parsed as CSV with delimiter specified.

**Specification**
```
  set format {type} {delimiter} {configuration}
```
* type - Currently the only type supported is CSV.
* delimiter - When type is CSV, the delimiter to be used for splitting into columns. If you would like to specify a
delimiter like a tab, then you specify it as '\\t'.
* configuration - Specifies configuration based on type, for CSV, ability to skip empty lines is specifiable.
The value can be either 'true' or 'false'.

**Example**
```
  set format csv , false
```

### Sed
A stream editor directive that can be used for performing basic text operations on the
column string to which it is applied.

**Specification**
```
  sed <column-name> <sed-script>
```

* column-name Specifies the name of the column on which the sed script is applied.
* sed-script Specifies the sed script to be applied to the column.

**Example**
```
  sed body s/"//g
```

### Changing Case

Directive that provides the ability to change the case of a column value. One can change the column value
 to uppercase, lowercase or titlecase.

**Specification**
```
 uppercase {column-name}
 lowercase {column-name}
 titlecase {column-name}
```
* column-name - Specifies the name of the column to which the changing case directives are applied.

**Example**
```
  uppercase state
  lowercase email
  titlecase name
```
### Drop a column

Drop a column directive will remove a column from the input record. The resulting output record will not
include the column specified in the directive.

**Specification**
```
  drop {column-name}
```

* column-name - name of the column to be dropped. If the column name doesn't exist, the processing is stopped.

**Example**
```
  drop zipcode
```

### Rename a column

Renames the name of the column.

**Specification**
```
  rename {source-column-name} {destination-column-name}
```
* source-column-name - Name of the column to be renamed. If the column name doesn't exist, the processing is stopped.
* destination-column-name - Name of the column to be set to.

**Example**
```
  rename email emailid
```
### Splitting Column

Often times there is need to split a column based on fixed indexes or based on a delimiter. The Wrangler
plugin support two ways to split a string.

* Based on start and end index &
* Based on delimiter

Index based split will take a source input column value and extract substring from start index to end index into
 a destination column name. This is mainly used for extracting substring from a source string.

```
  indexsplit {source-column-name} {start} {end} {destination-column-name}
```
**Specification**
* source-column-name - Name of the source column that needs to be split
* start - Start index to split. If start is less than 0, then it's defaulted to 0.
* end - End index to split. If end is greater than length of source-column-name value, it's defaulted to it's length.
* destination-column-name - Name of the column into which the value between start,end value from
source-column-name is stored.

**Example**
```
  indexsplit ssn 7 11 last4ssn
```

Delimiter based splitter would split the source column value based on delimiter into two columns.
First column will include the value to the left of the delimiter (excluding delimiter) and the
second column will hold the value to the right of the delimiter.

```
  split {source-column-name} {delimiter} {new-column-1} {new-column-2}
```
**Specification**
* source-column-name - Name of the source column that needs to be split
* delimiter - Delimiter to be used to split the source-column-name
* new-column-1 - Name of the new column that contains the substring left of delimiter. If the column doesn't
exist then it will be added. If it exists, it will replace.
* new-column-2 - Name of the new column that contains the substring right of delimiter. If the column doesn't
exist then it will be added. If it exists, it will replace.

**Example**
```
  split email @ name domain
```

### Specify column names

This directive specifies the name of the columns. After this directive is specified, the following
directives should use the new names of the columns specified by this directive.

**Specification**
```
  set columns {column-name-1},{column-name-2}, ... {column-name-3}
```
* {column-name-x} Specifies a list of column names to be assigned to column.

**Example**
```
  set columns id,fname,lname,email,address,city,state,zip
```

### Filter Row

Directive for filtering rows either based on a condition or based on regular expression. Upon execution of
this directive, the following directives would be excluded of the rows that were filtered by this directive.

Condition based filtering allows one to specify an expression that if results in 'true' would filter the row else
would pass the row as-is to the next directive.


**Specification**
```
  filter-row-if-true {condition}
```

* condition - A JEXL expression.

**Example**
```
  set columns id,fname,lname,email,address,city,state,zip
  filter-row-if-true id > 200
```

Regular expression based filtering applies an regular expression on the value of a column specified in the
directive. If the {condition} is true, the row will be skipped else it will be passed down to next step.
```
  filter-row-if-matched {column-name} {regex}
```

**Specification**
* column-name - Name of the column on which regex is applied. The regex is actually applied on the value of the column.
* regex - Standard regular expression.

**Example**
```
  set columns id,fname,lname,email,address,city,state,zip
  filter-row-if-matched email .*@joltie.io
```

### Set Column with expression
Set column directive allows you assign the result of a expression specified in JEXL format to a column.
JEXL implements an Expression Language for expressing not so complex expressions. Syntax support JEXL are
available [here](http://commons.apache.org/proper/commons-jexl/reference/syntax.html).

**Specification**
```
  set column {column-name} {expression}
```

* column-name - Name of the column to which the result of expression is saved to.
* expression - Expression to be evaluated specified in Jexl syntax.

**Example**
```
  set column salary hrlywage * 160
  set column hrlywage Math:abs(toDouble(hrlywage))
```

## Quantize
This directive quantizes a continous value of a column through a range table
specified. The quantization ranges are all real numbers, with low specifying the low end of the
 range and high specifying the high end of the range. Associated with the range is the
 value that if the incoming value falls in the range it would be assigned that value.
 The range is a closed range - [low:high] = {x | low <= x <= high}. Also, the high endpoint
 should be greater than low endpoint.

**Specification**
```
  quantize {source-column} {destination-column} {quantization-table}
```

* source-column : Name of the column which has to be quantized
* destination-column : Name of the column to which the quantized value should be added.
* quantization-table : Specifies the quantization table in the following format low:high=value[,low:high=value]*
the range specified in the quantization table is a closed range  and all the ranges specified are mutually exclusive.

**Example**
```
  quantize hrlywage wagecategory 0.0:4.99=LOW,5.0:13.99=NORMAL,14.0:29.99=HIGH,30.0:100.0=VERY HIGH
```

### Mask Column
Data masking (also known as data scrambling and data anonymization) is the process of replacing sensitive
information with realistic, but scrubbed, data based on masking rules. This plugin supports two types of
 masking method

* Substitution based &
* Shuffle based

Substitution based masking allows you to mask data based on a masking pattern. The patterns are specified using
two main literals namely '#' (Pound) and 'x'. '#' specifies that input should be passed on to output, 'x' would replace
the input charater with it. Any other characters will be passed as it to the output.
This directive is mainly used for masking SSN, customer id, credit card numbers, etc.

**Specification**

```
  mask-number {column-name} {masking-pattern}
```
* column-name - Name of the column to which the masking pattern needs to be applied
* masking-pattern - Defines the pattern to be used for masking the column.

**Example**
```
  mask-number ssn xxx-xx-####
  mask-number credircard xxxx-xxxxxx-x####
```

Shuffle based masking allows one to replace the input with the same size random data. It replaces
numbers with random numbers and string of characters with random characters.

**Specificagtion**
```
  mask-shuffle {column-name}
```

* column-name - Name of the column to be shuffle masked.

**Example**
```
  mask-shuffle address
```

### Date Transformation

Directive for transforming a date from one format to another, or for transforming from unix timetsamp to
a format of date.

To convert a date string from one format to another use the following directive.

**Specification**
```
  format-date {column-name} {source-date-format} {destination-date-format}
```
* column-name - Name of the column to convert from source to destination format.
* source-date-format - Specifies the format of date pattern.
* destination-date-format - Specifies the format of date pattern.

**Example**
```
  format-date date MM/dd/yyyy EEE, MMM d, ''yy
```

To convert from unix timestamp to a date format use the following directive
**Specificaton**

```
  format-unix-timestamp {column-name} {date-format}
```
* column-name - Name of the column that contains unix timestamp that needs to be converted to date-format
* date-format - Format to convert from unix timestamp.

**Example**
```
  format-unix-timestamp timestamp MM/dd/yyyy
```