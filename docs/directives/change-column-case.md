# Change Column case

CHANGE-COLUMN-CASE directive changes the column names to either uppercase or lowercase.

## Syntax

```
 change-column-case upper|lower
```

if case the type is incorrect or not specified, then it defaults to lowercase.

## Example

Let's look at how this work with an example

```
  {
    "Id" : 1,
    "Gender" : "male",
    "FNAME" : "Root",
    "lname" : "JOLTIE"
    "Address" : "67 MARS AVE, MARSCIty, Marsville, Mars"
  }
```

applying following directives

```
  change-column-case upper
```

would result in record as follows

```
  {
    "ID" : 1,
    "GENDER" : "MALE",
    "FNAME" : "Root",
    "LNAME" : "Joltie"
    "ADDRESS" : "67 mars ave, marscity, marsville, mars"
  }
```

applying following directives

```
  change-column-case lower
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

