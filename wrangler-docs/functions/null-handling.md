# Null Handling Functions

The collection of functions for handling nulls in your data.

## IsNotNull
Returns `true` when an expression does not evaluate to the `null` value.

#### Namespace
Global. No namespace qualifier required.

#### Input
any object or expression

#### Output
boolean (true/false)

#### Example
In the below example, `col1` will evaluate to `a` if `a` is not null, i.e, `col1` is `999` if `a` is `999`. 
If `a` is `null`, `col1` will evaluate to `0`. 

```
set-column col1 IsNotNull(a) ? a : 0
```

## IsNull
Returns `true` when an expression evaluates to the `null` value.

#### Namespace
Global. No namespace qualifier required.

#### Input
any object or expression

#### Output
boolean (true/false)

#### Example
In the below example, `col1` will evaluate to `a` if `a` is not null, i.e, `col1` is `999` if `a` is `999`. 
If `a` is `null`, `col1` will evaluate to `0`. 

```
set-column col1 IsNull(a) ? 0 : a
```

## NullToEmpty
Returns an `empty string` if the input column is `null`, otherwise returns the input column value.

#### Namespace
Global. No namespace qualifier required.

#### Input
any object or expression

#### Output
input value or string

#### Example
In the below example, `col1` will evaluate to `a` if `a` is not null, i.e, `col1` is `abc` if `a` is `abc`. 
If `a` is `null`, `col1` will evaluate to empty string `''`. 

```
set-column col1 NullToEmpty(a)
```

## NullToZero
Returns zero if the input column is `null`, otherwise returns the input column value.

#### Namespace
Global. No namespace qualifier required.

#### Input
column value (object/expression)

#### Output
column value or zero.

#### Example
In the below example, `col1` will evaluate to `a` if `a` is not null, i.e, `col1` is `999` if `a` is `999`. 
If `a` is `null`, `col1` will evaluate to `0`. 

```
set-column col1 NullToZero(a)
```

## NullToValue
Returns the specified value if the input column is `null`, otherwise returns the input column value.

#### Namespace
Global. No namespace qualifier required.

#### Input
column value (object/expression), replaced value (object/expression)

#### Output
column value or replaced value.

#### Example
In the below example, `col1` will evaluate to `a` if `a` is not null, i.e, `col1` is `999` if `a` is `999`. 
If `a` is `null`, `col1` will evaluate to the replaced value `42`. 

```
set-column col1 NullToValue(a, 42)
```