# Number Functions

The collection of functions for handling numbers in the data.

## AsDouble
Returns double value of the given argument. If the input is null, this method will return null.

#### Namespace
`number`

#### Input
number

#### Output
double

#### Example
If `a` contains the number `5.2`
then the following two functions are equivalent, and return the value `2.6`:

```
set-column val number:AsDouble(a/2)
set-column val number:AsDouble(5.2/2)
```

## AsFloat
Returns double value of the given argument. If the input is null, this method will return null.

#### Namespace
`number`

#### Input
number

#### Output
float

#### Example
If `a` contains the number `5.2`
then the following two functions are equivalent, and return the value `2.6`:

```
set-column val number:AsFloat(a/2)
set-column val number:AsFloat(5.2/2)
```

## AsInteger
Returns integer value of the given argument. If the input is null, this method will return null.

#### Namespace
`number`

#### Input
number

#### Output
integer

#### Example
If `a` contains the number `5.2`
then the following two functions are equivalent, and return the value `2`:

```
set-column val number:AsInteger(a/2)
set-column val number:AsInteger(5.2/2)
```

## Mantissa
Returns the mantissa from the given number. If the input is null, the method will return `0.0`.

#### Namespace
`number`

#### Input
number(integer, long, double, float or BigDecimal)

#### Output
double

#### Example
If `a` contains the number `123.4567`
then the following two functions are equivalent, and return the value `0.4567`:

```
set-column val number:Mantissa(a)
set-column val number:Mantissa(123.4567)
```