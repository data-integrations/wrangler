## Json

There are two directives for parsing and accessing different attributes of a JSON event. 

* [Parse Json](#parse-json)
* [Json Path](#json-path)

## Parse Json.

Directive for parsing a column value as Json. The directive can operate on String or JSONObject types. When the directive is applied on a String or JSONObject, the high-level keys of the json are appeneded to the original column name to create new column names. 

For example, let's say you have a simple json in record with field name ```body```
```
  {
    "id" : 1,
    "name" : {
      "first" : "Root",
      "last"  : "Joltie"
    },
    "age" : 22,
    "weigth" : 184,
    "height" : 5.8
  }
```
The application of first directive

```
parse-as-json body
```

Would generate following field names and field values

| Field Name | Field Values | Field Type |
| ------------- | ------------- | ----------------- |
| **body** | ```{ ... }``` | String |
| **body.id** | 1 | Integer |
| **body.name** | ```{ "first" : "Root", "last" : "Joltie" }``` | JSONObject |
| **body.age** | 22 | Integer |
| **body.weight** | 184 | Integer |
| **body.height** | 5.8 | Double |

Applying the same directive on field ```body.name``` generates the following results

| Field Name | Field Values | Field Type |
| ------------- | ------------- | ----------------- |
| **body** | ```{ ... }``` | String |
| **body.id** | 1 | Integer |
| **body.name** | ```{ "first" : "Root", "last" : "Joltie" }``` | JSONObject |
| **body.age** | 22 | Integer |
| **body.weight** | 184 | Integer |
| **body.height** | 5.8 | Double |
| **body.name.first** | "Root" | String |
| **body.name.last** | "Joltie" | String |

### Specification

```
parse-as-json {column-name}
```

| Argument      | Description |
| ------------- | ------------- |
| column-name   | Name of the column in the record to be parsed as JSON. |

### Example
```
  parse-as-json body
  parse-as-json body.deviceReference
  parse-as-json body.deviceReference.OS
  parse-as-csv  body.deviceReference.screenSize | true
  drop body.deviceReference.screenSize
  rename body.deviceReference.screenSize_col1 size1
  rename body.deviceReference.screenSize_col2 size2
  rename body.deviceReference.screenSize_col3 size3
  rename body.deviceReference.screenSize_col4 size4
  json-path body.deviceReference.alerts signal_lost $.[*].['Signal lost']
  json-path signal_lost signal_lost $.[0]
  drop body
  drop body.deviceReference.OS
  drop body.deviceReference
  rename body.deviceReference.timestamp timestamp
  set column timestamp timestamp / 1000000
  drop body.deviceReference.alerts
```

## Json Path

Directive that uses a DSL for reading Json records. The expressions always refer to a Json structure in the same way as XPath expression are used in combination with an XML document. The "root member object" is always referred to as ```$``` regardless if it is an object or array.

Expressions can use the dot–notation

```
$.name.first
```

or the bracket–notation

```
$['name']['first']
```

### Specification

```
json-path {column-name} {path-expression}
```

| Argument      | Description |
| ------------- | ------------- |
| column-name   | Name of the column in the record to be parsed as JSON. |
| path-expression | Specifies the DSL for reading Json records. Expression is relative to the object it's being operated on. |

### Example
```
json-path body.deviceReference.alerts signal_lost $.[*].['Signal lost']
```

Operators
---------

| Operator                  | Description                                                        |
| :------------------------ | :----------------------------------------------------------------- |
| `$`                       | The root element to query. This starts all path expressions.       |
| `@`                       | The current node being processed by a filter predicate.            |
| `*`                       | Wildcard. Available anywhere a name or numeric are required.       |
| `..`                      | Deep scan. Available anywhere a name is required.                  |
| `.<name>`                 | Dot-notated child                                                  |
| `['<name>' (, '<name>')]` | Bracket-notated child or children                                  |
| `[<number> (, <number>)]` | Array index or indexes                                             |
| `[start:end]`             | Array slice operator                                               |
| `[?(<expression>)]`       | Filter expression. Expression must evaluate to a boolean value.    |


Functions
---------

Functions can be invoked at the tail end of a path - the input to a function is the output of the path expression.
The function output is dictated by the function itself.

| Function                  | Description                                                        | Output    |
| :------------------------ | :----------------------------------------------------------------- |-----------|
| min()                    | Provides the min value of an array of numbers                       | Double    |
| max()                    | Provides the max value of an array of numbers                       | Double    |
| avg()                    | Provides the average value of an array of numbers                   | Double    |
| stddev()                 | Provides the standard deviation value of an array of numbers        | Double    |
| length()                 | Provides the length of an array                                     | Integer   |

Filter Operators
-----------------

Filters are logical expressions used to filter arrays. A typical filter would be `[?(@.age > 18)]` where `@` represents the current item being processed. More complex filters can be created with logical operators `&&` and `||`. String literals must be enclosed by single or double quotes (`[?(@.color == 'blue')]` or `[?(@.color == "blue")]`).   

| Operator                 | Description                                                       |
| :----------------------- | :---------------------------------------------------------------- |
| ==                       | left is equal to right (note that 1 is not equal to '1')          |
| !=                       | left is not equal to right                                        |
| <                        | left is less than right                                           |
| <=                       | left is less or equal to right                                    |
| >                        | left is greater than right                                        |
| >=                       | left is greater than or equal to right                            |
| =~                       | left matches regular expression  [?(@.name =~ /foo.*?/i)]         |
| in                       | left exists in right [?(@.size in ['S', 'M'])]                    |
| nin                      | left does not exists in right                                     |
| size                     | size of left (array or string) should match right                 |
| empty                    | left (array or string) should be empty                            |

Path Examples
-------------

Given the json

```javascript
{
    "store": {
        "book": [
            {
                "category": "reference",
                "author": "Nigel Rees",
                "title": "Sayings of the Century",
                "price": 8.95
            },
            {
                "category": "fiction",
                "author": "Evelyn Waugh",
                "title": "Sword of Honour",
                "price": 12.99
            },
            {
                "category": "fiction",
                "author": "Herman Melville",
                "title": "Moby Dick",
                "isbn": "0-553-21311-3",
                "price": 8.99
            },
            {
                "category": "fiction",
                "author": "J. R. R. Tolkien",
                "title": "The Lord of the Rings",
                "isbn": "0-395-19395-8",
                "price": 22.99
            }
        ],
        "bicycle": {
            "color": "red",
            "price": 19.95
        }
    },
    "expensive": 10
}
```

| JsonPath (click link to try)| Result |
| :------- | :----- |
| <a href="http://jsonpath.herokuapp.com/?path=$.store.book[*].author" target="_blank">$.store.book[*].author</a>| The authors of all books     |
| <a href="http://jsonpath.herokuapp.com/?path=$..author" target="_blank">$..author</a>                   | All authors                         |
| <a href="http://jsonpath.herokuapp.com/?path=$.store.*" target="_blank">$.store.*</a>                  | All things, both books and bicycles  |
| <a href="http://jsonpath.herokuapp.com/?path=$.store..price" target="_blank">$.store..price</a>             | The price of everything         |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[2]" target="_blank">$..book[2]</a>                 | The third book                      |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[0,1]" target="_blank">$..book[0,1]</a>               | The first two books               |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[:2]" target="_blank">$..book[:2]</a>                | All books from index 0 (inclusive) until index 2 (exclusive) |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[1:2]" target="_blank">$..book[1:2]</a>                | All books from index 1 (inclusive) until index 2 (exclusive) |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[-2:]" target="_blank">$..book[-2:]</a>                | Last two books                   |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[2:]" target="_blank">$..book[2:]</a>                | Book number two from tail          |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[?(@.isbn)]" target="_blank">$..book[?(@.isbn)]</a>          | All books with an ISBN number         |
| <a href="http://jsonpath.herokuapp.com/?path=$.store.book[?(@.price < 10)]" target="_blank">$.store.book[?(@.price < 10)]</a> | All books in store cheaper than 10  |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[?(@.price <= $['expensive'])]" target="_blank">$..book[?(@.price <= $['expensive'])]</a> | All books in store that are not "expensive"  |
| <a href="http://jsonpath.herokuapp.com/?path=$..book[?(@.author =~ /.*REES/i)]" target="_blank">$..book[?(@.author =~ /.*REES/i)]</a> | All books matching regex (ignore case)  |
| <a href="http://jsonpath.herokuapp.com/?path=$..*" target="_blank">$..*</a>                        | Give me every thing   
| <a href="http://jsonpath.herokuapp.com/?path=$..book.length()" target="_blank">$..book.length()</a>                 | The number of books                      |

