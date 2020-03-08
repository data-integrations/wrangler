# Null Handling Functions

The collection of functions for handling nulls in your data. 

## IsNotNull
Returns `true` when an expression does not evaluate to the `null` value.

## Namespace
Global. No namespace qualifier required. 

### Input
any object or expression

### Output
boolean (true/false)

### Example
If `a` contains the `null`, `b` contains string `value` and `c` contains number `999` then
If the input column does not contain a `null`, the output column contains the value of the input column. 
If the input column does contain a `null`, then the output column contains the string `null`. In the below
examples, `col1` is `null`, `col2` is `value`, `col3` is `999` and `col4` is `null`.

```
set-column col1 IsNotNull(a) ? a : null
set-column col2 IsNotNull(b) ? b : null
set-column col3 IsNotNull(c) ? c : null
set-column col4 if(IsNotNull(c)) { a } else { b }
```

## IsNull
Returns `true` when an expression evaluates to the `null` value.

## Namespace
Global. No namespace qualifier required. 

### Input
any object or expression

### Output
boolean (true/false)

### Example
If `a` contains the `null`, `b` contains string `value` and `c` contains number `999` then
If the input column does not contain a `null`, the output column contains the value of the input column. 
If the input column does contain a `null`, then the output column contains the string `null`. In the below
examples, `col1` is `null`, `col2` is `null`, `col3` is `null` and `col4` is `value`.

```
set-column col1 IsNull(a) ? a : null
set-column col2 IsNull(b) ? b : null
set-column col3 IsNull(c) ? c : null
set-column col4 if(IsNull(c)) { a } else { b }
```

## NullToEmpty
Returns an `empty string` if the input column is `null`, otherwise returns the input column value.

## Namespace
Global. No namespace qualifier required. 

### Input
any object or expression

### Output
input value or string

### Example
If `a` contains the `null`, `b` contains string `value` and `c` contains number `999` then
If the input column does not contain a `null`, the output column contains the value of the input column. 
If the input column does contain a `null`, then the output column contains the string `null`. In the below
examples, `col1` is `''`, `col2` is `value`, and `col3` is `999`.

```
set-column col1 NullToEmpty(a)
set-column col2 NullToEmpty(b)
set-column col3 NullToEmpty(c)
```

## NullToZero
Returns zero if the input column is `null`, otherwise returns the input column value.

## Namespace
Global. No namespace qualifier required. 

### Input
column value (object/expression)

### Output
column value or zero. 

### Example
If `a` contains the `null`, `b` contains string `value` and `c` contains number `999` then
If the input column does not contain a `null`, the output column contains the value of the input column. 
If the input column does contain a `null`, then the output column contains the string `null`. In the below
examples, `col1` is `0`, `col2` is `0`, and `col3` is `999`.

```
set-column col1 NullToZero(a)
set-column col2 NullToZero(b == 'value' ? a : b)
set-column col3 NullToZero(c)
```

## NullToValue
Returns zero if the input column is `null`, otherwise returns the input column value.

## Namespace
Global. No namespace qualifier required. 

### Input
column value (object/expression)

### Output
column value or zero. 

### Example
If `a` contains the `null`, `b` contains string `value` and `c` contains number `999` then
If the input column does not contain a `null`, the output column contains the value of the input column. 
If the input column does contain a `null`, then the output column contains the string `null`. In the below
examples, `col1` is `42`, `col2` is `42`, and `col3` is `999`.

```
set-column col1 NullToValue(a)
set-column col2 NullToValue(b == 'value' ? a : b, 42)
set-column col3 NullToValue(c)
```
