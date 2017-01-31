# Drop Column

DROP directive is used to drop a column from the record. 

## Syntax

```
 drop <column>[,<column>]*
```

```column``` is the name of the column in the record to be droped. If the ```column``` does not exist in the record, then the directive fails.

## Usage Notes

After the DROP directive is applied, the column and it's associated value are removed from the record. Downstream directives will not be able to reference the column there after.

## Example

Let's following is the record

```
  {
    "id" : 1,
    "timestamp" : 1234434343,
    "measurement" : 10.45,
    "isvalid" : true
  }
```

applying following DROP directive 

```
  drop isvalid,measurement
```

would result in record that no ```isvalid``` field. 

```
  {
    "id" : 1,
    "timestamp" : 1234434343
  }
```

