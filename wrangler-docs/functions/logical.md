# Logical Bitwise

The logical functions perform bit operations. All the functions described below are in the
`logical` namespace. To use the functions defined in this namespace prefix function invocation with `logical:<func>`.

## BitAnd
Returns the bitwise `AND` of the two arguments. The inputs must be non-null.

#### Namespace
`logical`

#### Input
number1(long), number2(long) or number1(int), number2(int)

#### Output
number(long if inputs are long, int if inputs are int)

#### Example
If `a` contains the number `352` and `b` contains the number `400`,
then the following two functions are equivalent, and return the value `256`:
```
set-column val logical:BitAnd(352, 400)
set-column val logical:BitAnd(a,b)
```

## BitOr
Returns the bitwise `OR` of the two arguments. The inputs must be non-null

#### Namespace
`logical`

#### Input
number1(long), number2(long) or number1(int), number2(int)

#### Output
number(long if inputs are long, int if inputs are int)

#### Example
If `a` contains the number `352` and `b` contains the number `400`,
then the following two functions are equivalent, and return the value `496`.

```
set-column val logical:BitOr(352,400)
set-column val logical:BitOr(a,b)
```

## BitXor
Returns the bitwise `XOR` of the two arguments. The inputs must be non-null.

#### Namespace
`logical`

#### Input
number1(long), number2(long) or number1(int), number2(int)

#### Output
number(long if inputs are long, int if inputs are int)

#### Example

If `a` contains the number `352` and `b` contains the number `400`,
then the following two functions are equivalent, and return the value `240`:

```
set-column val logical:BitXOr(352,400)
set-column val logical:BitXOr(a,b)
```

## BitCompress
Returns the `long` made from the string argument,
which contains a binary representation of `"1"s` and `"0"s`.

#### Namespace
`logical`

#### Input
String

#### Output
number(long)

#### Example
If `mynumber` contains the string `"101100000"`,
then the following two functions are equivalent, and return the number `352`.
```
set-column val logical:BitCompress("101100000")
set-column val logical:BitCompress(mynumber)
```

## BitExpand
Returns a `string` containing the binary representation in `"1"s` and `"0"s`
of the given `long`.

#### Namespace
`logical`

#### Input
number(long)

#### Output
String

#### Example
If `mynumber` contains the number `352`, then the following two functions
are equivalent, and return the string `"101100000"`
```
set-column val logical:BitExpand(352)
set-column val logical:BitExpand(mynumber)
```

## Not
Returns the Not of the logical value of an expression. If the value of expression is `true`, the Not function
returns a value of `false (0)`. If the value of expression is `false`, the `NOT` function returns a value of `true (1)`.
If a numeric expression evaluates to `0`, the method will return `1`. For any other number, the method will return `0`.
If a String is null or empty, the method will return `1`. For any other string, the method will return `0`.

#### Namespace
`logical`

#### Input
expression(double, float, int, long, String)

#### Output
number(int)

#### Example
If the below example, the first one will return 1, the second one will return 0, the third one will return 0.

```
set-column val logical:Not(5-5)
set-column val logical:Not(5+5)
set-column val logical:Not('value')
```

## SetBit
Returns a `long` with specific bits set to a specific state, where `'origValue'` is the input value to perform
the action on, `'bitArray'` is a array containing a list bit numbers (starting from 1) to set the state of,
and `'bitState'` is either `1` or `0`, indicating which state to set those bits.

#### Namespace
`logical`

#### Input
origValue (long), bitArray (int[]), bitState (1 or 0)

#### Output
number(long)

#### Example
If `origValue` contains the number `352`, `bitArray` contains the list `[2,4,8]`, and `bitState`
contains the value `1`, then the following two functions are equivalent, and return the value `494`:

```
set-column val logical:SetBit(356, [2,4,8], 1)
set-column val logical:SetBit(origValue, bitArray, bitState)
```
