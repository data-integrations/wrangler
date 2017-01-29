# Cut

CUT directive selects parts of the string value.

## Syntax

```
 cut <source> <destination> -c [<range>|<indexes>]
 cut <source> <destination> -d <delimiter> -f [<range>|<indexes>]
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
  cut body one -c 1-3
  cut body two -c 5-7
  cut body three -c 9-13
  cut body four -c 15-
  cut body five -c 1,2,3
  cut body six -c -3
  cut body seven -c 1,2,3-5
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

