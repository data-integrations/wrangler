# Set Columns

SET COLUMNS directive sets the name of the columns as specified in the order they were specified.

## Syntax

```
 set columns <columm>[,<column>]*
```

```column``` specifies the name of the column to be set.

## Usage Notes

The most common use of SET COLUMNS directive is to set the name of columns when
we parse a CSV file. The column names will be applied to the record start from
field zero in the order they were specified.

Let's take a simple example, let's say you have parsed a 'body' using [PARSE-AS-CSV](csv-parser.md)
directive and then you are applying SET COLUMNS directive.


```
  {
    "body" : "1,2,3,4,5"
  }
```

Applying the directives below

```
  parse-as-csv body , true
  set columns a,b,c,d,e
```

Would generate record that has the column names assigned as follows:

```
{
  "a" : "1,2,3,4,5",
  "b" : "1",
  "c" : "2",
  "d" : "3",
  "e" : "4",
  "body_5" : "5"   ---> Note this was not assigned the name as expected.
}
```

In order to make this right, you would have to add a [DROP](drop.md) directive to the mix. So, it would be as follows:

```
  parse-as-csv body , true
  drop body
  set columns a,b,c,d,e
```

Now, you'r record would look as follows:

```
{
  "a" : "1",
  "b" : "2",
  "c" : "3",
  "d" : "4",
  "e" : "5"
}
```

### Most common mistake

When using SET COLUMNS directive, the number of fields in the record should be same
as number of column names in the SET COLUMNS directive. If they are not, then this
directive will partially name the record fields.

So, when this directive is executed in the Wrangler Transform and if the field "Name of field"
to be transformed is set to '*' then all fields are added to the record causing issues with
naming of the columns -- as it would also include column names that are coming from the
input.
