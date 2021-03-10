# Timestamp To Datetime

The TIMESTAMP-TO-DATETIME directive converts a timestamp value to datetime


## Syntax
```
timestamp-to-datetime <colname>
```


## Usage Notes

The TIMESTAMP-TO-DATETIME directive converts timestamp values to 
datetime values .
 
If the column is `null` applying this directive is a no-op. 

## Examples
timestamp-to-datetime :col1