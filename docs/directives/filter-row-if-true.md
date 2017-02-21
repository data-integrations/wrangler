# Filter Row based on condition

FILTER-ROW-IF-TRUE directive provides support for filters records that match the condition.

## Deprecated

Please use [FILTER-ROWS-ON](filter-rows-on.md) directive.

## Syntax
```
  filter-row-if-true <condition>
```

```condition``` is a valid boolean expression resulting in either ```true``` or ```false```.

## Usage Notes

The FILTER-ROW-IF-TRUE directive evaluates the boolean condition for each record. If the result of evaluation is ```true```,
 it skips the record, else it passes the record as-is to the input of next directive.

```condition``` is specified in JEXL expression language. The JEXL context is decorated with ```Math``` and
```StringUtils``` libraries for additional. The additional utility libraries are defined in 'math' and 'string'
namespace.

For more information on how to write JEXL expression please refer [here](https://commons.apache.org/proper/commons-jexl/reference/syntax.html).

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

Low let's say you want to filter out individual within US who's ```hrlywage``` is greater than 12.

```
  filter-row-if-true (country !~ US && hrlywage > 12)
```

Will result in records that have individual who reside in US and who's hourly wage is less than 12.
