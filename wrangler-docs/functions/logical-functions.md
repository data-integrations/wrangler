# Logical Bitwise Functions

The logical functions perform bit operations.

## BitAnd
Returns the bitwise AND of the two integer arguments.

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