# Logical Bitwise Functions

The logical functions perform bit operations.

## BitAnd
Returns the bitwise AND of the two long arguments.

### Input
number1(long), number2(long)

### Output
long

### Example
If a contains the number 352 and b contains the number 400, then the following two functions are equivalent, and return the value 256:
```
set-column val logical:BitAnd(352, 400)
set-column val logical:BitAnd(a,b)
```

---

## BitOr
Returns the bitwise OR of the two long arguments.

### Input

### Output

### Example

```
set-column val logical:BitOr(352,400)
set-column val logical:BitOr(a,b)
```

---

## BitXOr
Returns the bitwise XOR of the two long arguments.

### Input

### Output

### Example

```
set-column val logical:BitXOr(352,400)
set-column val logical:BitXOr(a,b)
```

---

## BitCompress
Returns the integer made from the string argument, which contains a binary representation of "1"s and "0"s.

### Input
String

### Output
number(long)

### Example
If mynumber contains the string "0101100000", then the following two functions are equivalent, and return the number 352.
```
set-column val logical:BitCompress("0101100000")
set-column val logical:BitCompress(mynumber)
```

---

## BitExpand
Returns a string containing the binary representation in "1"s and "0"s of the given long.

### Input
number(long)

### Output
String

### Example
If mynumber contains the number 352, then the following two functions are equivalent, and return the string "101100000"
```
set-column val logical:BitExpand(352)
set-column val logical:BitExpand(mynumber)
```
