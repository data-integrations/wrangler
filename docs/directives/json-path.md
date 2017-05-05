# JSON Path

The JSON-PATH directive uses a DSL for reading JSON records.


## Syntax
```
json-path <source-column> <destination-column> <expression>
```

* `<source-column>` specifies the column in the record that should be considered as the
  "root member object" or "$"
* `<destination-column>` is the name of the output column in the record where the results of
  the expression will be stored
* `<expression>` is a JSON path expression; see _Usage Notes_ below for details


## Usage Notes

An expression always refers to a JSON structure in the same way that an XPath expression
is used in combination with an XML document. The "root member object" is always referred
to as `$` regardless if it is an object or an array.


### Notation

Expressions can use either the "dot–notation":
```
$.name.first
```

or the "bracket–notation":
```
$['name']['first']
```


### Operators

| Operator                  | Description                                                 |
| ------------------------- | ----------------------------------------------------------- |
| `$`                       | The root element to query; this starts all path expressions |
| `@`                       | The current node being processed by a filter predicate      |
| `*`                       | Wildcard; available anywhere a name or numeric are required |
| `..`                      | Deep scan; available anywhere a name is required            |
| `.<name>`                 | Dot-notated child                                           |
| `['<name>' (, '<name>')]` | Bracket-notated child or children                           |
| `[<number> (, <number>)]` | Array index or indexes                                      |
| `[start:end]`             | Array slice operator                                        |
| `[?(<expression>)]`       | Filter expression; must evaluate to a boolean value         |


### Functions

Functions can be invoked at the tail end of a path: the input to a function is the output
of the path expression. The function output is dictated by the function itself.

| Function   | Returns                                             | Output  |
| ---------- | --------------------------------------------------- | ------- |
| `min()`    | The min value of an array of numbers                | Double  |
| `max()`    | The max value of an array of numbers                | Double  |
| `avg()`    | The average value of an array of numbers            | Double  |
| `stddev()` | The standard deviation value of an array of numbers | Double  |
| `length()` | The length of an array                              | Integer |


### Filter Operators

Filters are logical expressions used to filter arrays. A typical filter would be:
```
[?(@.age>18)]
```

where `@` represents the current item being processed.

* More complex filters can be created with the logical operators `&&` and `||`

* String literals must be enclosed by either single or double quotes, such as in
  `[?(@.color=='blue')]` or `[?(@.color=="blue")]`

| Filter Operator | Description                                                               |
| --------------- | ------------------------------------------------------------------------- |
| `==`            | Left is equal in type and value to right (note `1` is not equal to `'1'`) |
| `!=`            | Left is not equal to right                                                |
| `<`             | Left is less than right                                                   |
| `<=`            | Left is less than or equal to right                                       |
| `>`             | Left is greater than right                                                |
| `>=`            | Left is greater than or equal to right                                    |
| `=~`            | Left matches regular expression `[?(@.name=~/foo.*?/i)]`                  |
| `in`            | Left exists in right `[?(@.size in ['S', 'M'])]`                          |
| `nin`           | Left does not exist in right                                              |
| `size`          | Size of left (array or string) matches right                              |
| `empty`         | Left (array or string) is empty                                           |


## Examples

Using this record as an example:
```json
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


| JSON Path (click link to test)                                                                                 | Result                                                       |
| -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| [$.store.book[*].author](http://jsonpath.herokuapp.com/?path=$.store.book[*].author)                           | The authors of all books                                     |
| [$..author](http://jsonpath.herokuapp.com/?path=$..author)                                                     | All authors                                                  |
| [$.store.*](http://jsonpath.herokuapp.com/?path=$.store.*)                                                     | All things, both books and bicycles                          |
| [$.store..price](http://jsonpath.herokuapp.com/?path=$.store..price)                                           | The price of everything                                      |
| [$..book[2]](http://jsonpath.herokuapp.com/?path=$..book[2])                                                   | The third book                                               |
| [$..book[0,1]](http://jsonpath.herokuapp.com/?path=$..book[0,1])                                               | The first two books                                          |
| [$..book[:2]](http://jsonpath.herokuapp.com/?path=$..book[:2])                                                 | All books from index 0 (inclusive) until index 2 (exclusive) |
| [$..book[1:2]](http://jsonpath.herokuapp.com/?path=$..book[1:2])                                               | All books from index 1 (inclusive) until index 2 (exclusive) |
| [$..book[-2:]](http://jsonpath.herokuapp.com/?path=$..book[-2:])                                               | Last two books                                               |
| [$..book[2:]](http://jsonpath.herokuapp.com/?path=$..book[2:])                                                 | Book number two from tail                                    |
| [$..book[?(@.isbn)]](http://jsonpath.herokuapp.com/?path=$..book[?(@.isbn)])                                   | All books with an ISBN number                                |
| [$..book[?(@.isbn)]](http://jsonpath.herokuapp.com/?path=$..book[?(@.isbn)])                                   | All books with an ISBN number                                |
| [$.store.book[?(@.price<10)]](http://jsonpath.herokuapp.com/?path=$.store.book[?(@.price<10)\])                | All books in store cheaper than 10                           |
| [$..book[?(@.price<=$['expensive'])]](http://jsonpath.herokuapp.com/?path=$..book[?(@.price<=$['expensive'])]) | All books in store that are not "expensive"                  |
| [$..book[?(@.author=~/.*REES/i)]](http://jsonpath.herokuapp.com/?path=$..book[?(@.author=~/.*REES/i)])         | All books matching a regex (ignore case)                     |
| [$..*](http://jsonpath.herokuapp.com/?path=$..*)                                                               | All books                                                    |
| [$..book.length()](http://jsonpath.herokuapp.com/?path=$..book.length())                                       | The number of books                                          |
