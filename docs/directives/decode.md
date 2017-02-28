# Decode A Column

DECODE directive decodes a column value as BASE32, BASE64 or HEX as per RFC-4648.

## Syntax
```
  decode <base32|base64|hex> <column>
```

```column``` is the name of the column to which decoding is applied.


## Usage Notes

Base decoding of data is used in many situations to store or transfer
data in environments that, perhaps for legacy reasons, are restricted
to US-ASCII [1] data.  Base encoding can also be used in new
applications that do not have legacy restrictions, simply because it
makes it possible to manipulate objects with text editors.

Upon using DECODE directive, it generates a new column were name
would be of following format:

```<column>_decode_<type>```

Following is how this directive will handle different column values.

* If the column is 'null', the resulting column
will also be 'null'.
* If a column specified is not found in the record, then it skips the record
with no-op and moves to the next record.
* If the column value is not of type string or byte array then it fails.

## Example

Let's assume the following record

```
  {
    "col1" : "IJQXGZJTGIQEK3TDN5SGS3TH",
    "col2" : "VGVzdGluZyBCYXNlIDY0IEVuY29kaW5n",
    "col3" : "48657820456e636f64696e67"
  }
```

applying the directives as follows:

```
  decode base32 col1
  decode base64 col2
  decode hex col3
```

Will generate the following record


```
  {
    "col1" : "IJQXGZJTGIQEK3TDN5SGS3TH",
    "col2" : "VGVzdGluZyBCYXNlIDY0IEVuY29kaW5n",
    "col3" : "48657820456e636f64696e67",
    "col1_decode_base32" : "Base32 Encoding",
    "col2_decode_base64" : "Testing Base 64 Encoding",
    "col3_decode_hex" : "Hex Encoding",

  }
```
