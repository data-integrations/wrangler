# Flatten

FLATTEN directive separates the elements in a repeated field into individual records.

## Syntax 

```
 flatten <column>[, <column> ]*
```

```column``` name of the column that is a JSON Array.

## Usage Notes

The FLATTEN function is useful for flexible exploration of repeated data.

To maintain the association between each flattened value and the other fields in the record, the FLATTEN function copies all of the other columns into each new record.

In order to understand how FLATTEN directive works in different scenarios, let's look at them with examples. 

### Case 1 

The array in ```col2``` is flattened, the values in ```col3``` is repeated for every value of ```col2```

**INPUT**
```
 [
   { "col1" : "A" },
   { "col1" : "B" },
   { "col2" : [x1, y1], "col3" : 10 },
   { "col2" : [x2, y2], "col3" : 11 } 
 ]   
```
**OUTPUT**
```
 [
   { "col1" : "A" },
   { "col1" : "B" },
   { "col2" : "x1", "col3" : 10 },
   { "col2" : "y1", "col3" : 10 },
   { "col2" : "x2", "col3" : 11 },
   { "col2" : "y2", "col3" : 11 }   
 ]
```

### Case 2

The array in ```col2``` and ```col3``` are flattened. 

**INPUT**
```
 [
   { "col1" : "A" },
   { "col1" : "B" },
   { "col2" : [ "x1", "y1", "z1" ], "col3" : [ "a1", "b1", "c1" ] },
   { "col2" : [ "x2", "y2" ], "col3" : [ "a2", "b2" ] }
 ]
```
**OUTPUT**
```
 [
   { "col1" : "A" },
   { "col1" : "B" },
   { "col2" : "x1", "col3" : "a1" },
   { "col2" : "y1", "col3" : "b1" },
   { "col2" : "z1", "col3" : "c1" },
   { "col2" : "x2", "col3" : "a2" },
   { "col2" : "y2", "col3" : "b2" } 
 ]
```
### Case 3

The array in ```col2``` and ```col3``` are flattened. 

**INPUT**
```
 [
   { "col1" : "A" },
   { "col1" : "B" },
   { "col2" : [ "x1", "y1", "z1" ], "col3" : [ "a1", "b1" ] },
   { "col2" : [ "x2", "y2" ], "col3" : [ "a2", "b2", "c2" ] }
 ]
```
**OUTPUT**
```
 [
   { "col1" : "A" },
   { "col1" : "B" },
   { "col2" : "x1", "col3" : "a1" },
   { "col2" : "y1", "col3" : "b1" },
   { "col2" : "z1" },
   { "col2" : "x2", "col3" : "a2" },
   { "col2" : "y2", "col3" : "b2" },
   { "col3" : "c2" }
 ]
```

## Examples

A very simple example would turn this data (one record):

```
{
  "x" : 5,
  "y" : "a string",
  "z" : [ 1,2,3]
}
```

into three distinct records:

| x           | y              | z         |
|-------------|----------------|-----------|
| 5           | "a string"     | 1         |
| 5           | "a string"     | 2         |
| 5           | "a string"     | 3         |

The function takes a single argument, which must be an array (the z column in this example). Using the all (*) wildcard as the argument to flatten is not supported and returns an error.
