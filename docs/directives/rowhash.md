# RowHash

The ROWHASH directive generates a hash value from the contents of the entire record.


## Syntax
```
rowhash <column> <codec>
```

* `<column>` is the name of the column to create with the hash value
* `<codec>` is the codec type.  Possible values include md5, sha1, sha128, sha256, and sha512.


## Usage Notes

The ROWHASH directive will generates a hash value from the contents of the record.  You can use the hash value to identify duplicate records.
Te hash value will be written to the column you specify.



## Example

Using this record as an example:
```
{
  "column1": "Ciao",
  "column2": "Hello World!",
}

++++++++++++++++++++++++++++++++
|  column1  |     column2      |
++++++++++++++++++++++++++++++++
|    Ciao   |   Hello World!   |
++++++++++++++++++++++++++++++++
```

Applying the directive:
```
rowhash column3 md5
```

would result in this record:
```
{
  "column1": "Ciao",
  "column2": "Hello World!",
  "column3": "433C37E24206BAC8BA49D23E8A3D4297",
}

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
|  column1  |     column2      |            column3               |
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
|    Ciao   |   Hello World!   | 433C37E24206BAC8BA49D23E8A3D4297 |
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
```
