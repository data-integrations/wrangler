# Encode A Column

ENCODE directive encodes a column value as BASE32, BASE64 or HEX as per RFC-4648.

## Syntax
```
  encode <base32|base64|hex> <column>
```

```column``` is the name of the column to which encoding is applied.


## Usage Notes

Base encoding of data is used in many situations to store or transfer
data in environments that, perhaps for legacy reasons, are restricted
to US-ASCII [1] data.  Base encoding can also be used in new
applications that do not have legacy restrictions, simply because it
makes it possible to manipulate objects with text editors.

Upon using ENCODE directive, it generates a new column were name
would be of following format: **&lt;column&gt;_encode_&lt;type&gt;**

Following is how this directive will handle different column values.

* If the column is 'null', the resulting column will also be 'null'.
* If a column specified is not found in the record, then it skips the record
with no-op and moves to the next record.
* If the column value is not of type string or byte array then it fails.

## Example

Let's assume the following record

```
  {
    "col1" : "Base32 Encoding",
    "col2" : "Testing Base 64 Encoding",
    "col3" : "Hex Encoding"
  }
```

applying the directives as follows:

```
  encode base32 col1
  encode base64 col2
  encode hex col3
```

Will generate the following record


```
  {
    "col1" : "Base32 Encoding",
    "col2" : "Testing Base 64 Encoding",
    "col3" : "Hex Encoding",
    "col1_encode_base32" : "IJQXGZJTGIQEK3TDN5SGS3TH",
    "col2_encode_base64" : "VGVzdGluZyBCYXNlIDY0IEVuY29kaW5n",
    "col3_encode_hex" : "48657820456e636f64696e67"

  }
```
