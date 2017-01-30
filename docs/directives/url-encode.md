# Generate UUID

URL-ENCODE encodes a string to application/x-www-form-urlencoded MIME format.

## Syntax
```
  url-encode <column>
```

```column``` to be url encoded.

## Usage Notes

When encoding a string, the following rules apply:

* The alphanumeric characters "a" through "z", "A" through "Z" and "0" through "9" remain the same.
*  The special characters ".", "-", "*", and "_" remain the same.
* The space character " " is converted into a plus sign "+".
* All other characters are unsafe and are first converted into one or more bytes using some encoding scheme.
Then each byte is represented by the 3-character string "%xy", where xy is the two-digit hexadecimal representation
of the byte. The recommended encoding scheme to use is UTF-8. However, for compatibility reasons, if an encoding
is not specified, then the default encoding of the platform is used.

> NOTE: Uses UTF-8 as the encoding scheme for the string.
