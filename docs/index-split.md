# Index Split

INDEXSPLIT directive splits a string
UPPERCASE, LOWERCASE and TITLECASE directives change the case of the column value they are applied on.
on.

## Syntax

```
 uppercase <column>
 lowercase <column>
 titlecase <column>
```

The directive perform in-place change of case.

## Example

Let's look at how this work with an example

```
  {
    "id" : 1,
    "gender" : "male",
    "fname" : "Root",
    "lname" : "JOLTIE"
    "address" : "67 MARS AVE, MARSCIty, Marsville, Mars"
  }
```

applying following directives

```
  uppercase gender
  titlecase lname
  lowercase address
```

would result in record as follows

```
  {
    "id" : 1,
    "gender" : "MALE",
    "fname" : "Root",
    "lname" : "Joltie"
    "address" : "67 mars ave, marscity, marsville, mars"
  }
```

