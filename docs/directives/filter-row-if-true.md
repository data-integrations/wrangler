# Filter Row based on condition

FILTER-ROW-IF-TRUE directive filters rows that match the condition specified.

## Syntax
```
  filter-row-if-true <condition>
```

```condition``` is a valid expression resulting in either ```true``` or ```false```.

## Usage Notes

The FILTER-ROW-IF-TRUE directive applies the condition on each Record and if the condition evaluates to ```true```
it skips emitting the record. If the condition evaluates to ```false``` it will pass the record as-is to the
next Step in the pipeline.

The ```condition``` specified should be a valid JEXL expression. The JEXL expression includes the
 ```Math``` library in the namespace 'math' and ```StringUtils``` library in the namespace 'string'. There are
 different convertors available that are included in the default namespace.

 For more information on what is available as part of expression for condition please refer
 [here](docs/directive/expression.md).

## Examples

Let's illustrate how this directive works with a concrete example.
Let's assume that you have input records as follows:

```
  {
    "id" : 1
    "name" : "Joltie, Root",
    "hrlywage" : "12.34",
    "gender" : "Male",
    "country" : "US",
  }
```

You want to operate only on the records where an individual is residing in ```country``` US.

```
  filter-row-if-true country !~ US
```

Will result in filtering out records for individuals that are not in US.
