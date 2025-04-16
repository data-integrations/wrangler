# Set Column

The SET-COLUMN directive sets the column value to the result of an expression execution.


## Syntax
```
set-column <columm> <expression>
```

* The `<column>` specifies the name of a column. If the column exists already, its value will be overwritten with the result of the specified expression. If the column does not exist, a new column will be created with the result of the specified expression.
* The `<expression>` is a valid [Apache Commons JEXL
  expression](http://commons.apache.org/proper/commons-jexl/reference/syntax.html)


## Usage Notes

The SET-COLUMN directive sets the column value to the result of the execution of an expression.

Expressions are written in [Apache Commons
JEXL](http://commons.apache.org/proper/commons-jexl/reference/syntax.html) notation.

Functions from other namespaces (such as `string` and `math`) can be called by adding the
namespace and a colon before the function, such as `math:ceil` or `string:upperCase`.


## Examples

Using this record as an example:
```
{
  "first": "Root",
  "last": "Joltie",
  "age": "32",
  "hrlywage": "11.79",
}
```

Applying these directives:
```
set-column name concat(last, ", ", first)
set-column is_adult age > 21 ? 'yes' : 'no'
set-column salary hrlywage*40*50
set column raised_hrlywage var x; x = math:ceil(FLOAT(hrlywage)); x + 1
set column salutation string:upperCase(concat(first, ' ', last))
```

would result in this record:
```
{
  "first": "Root",
  "last": "Joltie",
  "age": "32",
  "hrlywage": "11.79",
  "name": "Joltie, Root",
  "is_adult": "yes",
  "salary": 23580.0,
  "raised_hrlywage": 13.0,
  "salutation": "ROOT JOLTIE"
}
```

## Arithmetic and decimal operations example 

Arithmetic operations can be used in several ways. 
- To apply a simple arithmetic operation to a single, non-Decimal column, use mathematical notation; for example,
```
set-column :output wage1 * 2
```
- To apply an operation to a single column of type Decimal, use the `decimal` operations; for example,
```
set-column :decimal_op_1 decimal:add(wage_1,25)
```
- To apply operations to multiple columns (of any type), use the `arithmetic` operations, e.g. 
```
set-column :arithmetic_op_1 arithmetic:add(wage_1,wage_2)
```
For more information on working with numbers in Wrangler, see https://cdap.atlassian.net/wiki/spaces/DOCS/pages/413466692/Working+with+numbers.

Using this record as an example:
```
{
  "first": "Root",
  "last": "Joltie",
  "id": "10097",
  "age": "32",
  "wage_1": "11.79",
  "wage_2": "21151.26"
  "wage_3": "1794"
  "wage_4": "25000.98",
  "wage_5": "2.5",
  "wage_6": "5400",
  "wage_7": "9820.49",
  "wage_8": "520",
  "wage_9": "16697.200"
}
```
Applying these directives:
```
set-column :arithmetic_op_1 arithmetic:add(wage_1,wage_2)
set-column :arithmetic_op_2 arithmetic:minus(wage_4,wage_3)
set-column :arithmetic_op_3 arithmetic:multiply(wage_3,wage_4)
set-column :arithmetic_op_4 arithmetic:divideq(wage_4,wage_5)
set-column :arithmetic_op_5 arithmetic:divider(wage_5,wage_6)
set-column :arithmetic_op_6 arithmetic:lcm(wage_6,wage_7)
set-column :arithmetic_op_7 arithmetic:equal(wage_7,wage_8)
set-column :arithmetic_op_8 arithmetic:max(wage_7,wage_8)
set-column :arithmetic_op_9 arithmetic:min(wage_7,wage_8)
set-column :decimal_op_1 decimal:add(wage_1,25)
set-column :decimal_op_2 decimal:subtract(wage_2,1000)
set-column :decimal_op_3 decimal:multiply(wage_3,3)
set-column :decimal_op_4 decimal:divideq(wage_4,2)
set-column :decimal_op_5 decimal:divider(wage_5,5)
set-column :decimal_op_6 decimal:abs(wage_6)
set-column :decimal_op_7 decimal:precision(wage_7)
set-column :decimal_op_8 decimal:scale(wage_7)
set-column :decimal_op_9 decimal:unscaled(wage_7)
set-column :decimal_op_10 decimal:decimal_left(wage_7,1)
set-column :decimal_op_11 decimal:decimal_right(wage_7,1)
set-column :decimal_op_12 decimal:pow(wage_8,2)
set-column :decimal_op_13 decimal:negate(wage_8)
set-column :decimal_op_14 decimal:strip_zero(wage_9)
set-column :decimal_op_15 decimal:sign(wage_9)
set-type :wage_2 decimal
```
would result in this record:
```
{
  "first": "Root",
  "last": "Joltie",
  "id": "10097",
  "age": "32",
  "wage_1": "11.79",
  "wage_2": "21151.26"
  "wage_3": "1794"
  "wage_4": "25000.98",
  "wage_5": "2.5",
  "wage_6": "5400",
  "wage_7": "9820.49",
  "wage_8": "520",
  "wage_9": "16697.200",
  "arithmetic_op_1": "21163.05",
  "arithmetic_op_2": "23206.98",
  "arithmetic_op_3": "44851758.12",
  "arithmetic_op_4": "10000.392",
  "arithmetic_op_5": "2.5",
  "arithmetic_op_6": "5303064600",
  "arithmetic_op_7": "False",
  "arithmetic_op_8": "9820.49",
  "arithmetic_op_9": "520",
  "decimal_op_1": "36.79",
  "decimal_op_2": "20151.26",
  "decimal_op_3": "5382",
  "decimal_op_4": "12500.49",
  "decimal_op_5": "2.5",
  "decimal_op_6": "5400",
  "decimal_op_7": "6",
  "decimal_op_8": "2",
  "decimal_op_9": "982049",
  "decimal_op_10": "982.049",
  "decimal_op_11": "98204.9",
  "decimal_op_12": "270400",
  "decimal_op_13": "-520",
  "decimal_op_14": "16697.2",
  "decimal_op_15": "1"
}
```
