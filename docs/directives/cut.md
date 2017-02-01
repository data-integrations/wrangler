# Cut

CUT directive selects parts of the string value.

## Syntax

```
 cut-character <source> <destination> <range>|<indexes>
```

## Usage Notes


## Example

Let's following is the record

```
  {
    "body" : "one two three four five six seven eight"
  }
```

applying following CUT directive

```
  cut-character body one -c 1-3
  cut-character body two -c 5-7
  cut-character body three -c 9-13
  cut-character body four -c 15-
  cut-character body five -c 1,2,3
  cut-character body six -c -3
  cut-character body seven -c 1,2,3-5
```

would result in record as show below

```
  {
    "body" : "one two three four five six seven eight",
    "one" : "one",
    "two" : "two",
    "three" : "three",
    "four" : "four five six seven eight",
    "five" : "one",
    "six" : "one",
    "seven" : "one t"
  }
```

