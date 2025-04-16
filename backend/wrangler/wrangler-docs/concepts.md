# DataPrep Concepts

This implementation of Data Prep uses the concepts of _Record_, _Column_, _Directive_,
_Step_, and _Pipeline_.

### Recipe

A *Recipe* is a collection of *Directive*. It consists of one or more *Directive*.

### Directive

A *Directive* is a single data manipulation instruction, specified to either transform,
filter, or pivot a single record into zero or more records. A directive can generate one
or more *steps* to be executed by a pipeline.

### Row

A *Row* is a collection of field names and field values.

### Column

A *Column* is a data value of any of the supported Java types, one for each record.

### Pipeline

A *Pipeline* is a collection of steps to be applied on a record. The record(s) outputed
from a step are passed to the next step in the pipeline.

## Notations

### Directives

A directive can be represented in text in this format:

```
<command> <argument-1> <argument-2> ... <argument-n>
```

### Row

A row in this documentation will be shown as a JSON object with an object key
representing the column names and a value shown by the plain representation of the
the data, without any mention of types.

For example:

```
{
  "id": 1,
  "fname": "root",
  "lname": "joltie",
  "address": {
    "housenumber": "678",
    "street": "Mars Street",
    "city": "Marcity",
    "state": "Maregon",
    "country": "Mari"
  },
  "gender": "M"
}
```
