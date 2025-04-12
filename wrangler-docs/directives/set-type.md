# Set Type

Convert data type of a column 

## Syntax
```
set-type <column> <type> [<scale> <rounding-mode> prop:{precision=<precision>}]
```
The `<column>` is converted to the type in `<type>`.
Acceptable types are: int, short, long, float, double, decimal, string, bytes, boolean.
When `decimal` type is specified, two optional arguments can be given:
- `<scale>`: set the scale of the decimal value. 
- `<rounding-mode>`: Java [rounding-mode](https://docs.oracle.com/javase/7/docs/api/java/math/RoundingMode.html) 
to use when decimal value's scale is not equal to the set scale. By default, `HALF_EVEN` is assumed.
- `<precision>`: set the precision of the decimal value.