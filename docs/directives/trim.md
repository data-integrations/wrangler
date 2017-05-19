# Changing Case

The TRIM, LTRIM, and RTRIM directives trim whitespace from both sides,
left side or right side of a string values they are applied to.


## Syntax
```
trim <column>
ltrim <column>
rtrim <column>
```

The directive performs an in-place change of case.


## Example

Using this record as an example:
```
{
  "id": 1,
  "gender": "    male    ",
  "fname": "    Root    ",
  "lname": "   JOLTIE   ",
  "address": "    67 MARS AVE, MARSCIty, Marsville, Mars"
}
```

Applying these directives
```
trim gender
ltrim fname
rtrim lname
ltrim address
```

would result in this record:
```
{
  "id": 1,
  "gender": "male",
  "fname": "Root    ",
  "lname": "   JOLTIE",
  "address": "67 MARS AVE, MARSCIty, Marsville, Mars"
}
```
