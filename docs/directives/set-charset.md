# Setting Character set encoding

SET-CHARSET directive sets the encoding of the current data to easily
convert it to a String.

## Syntax

```
 set-charset <column> <charset>
```

## Usage Notes

This directive sets the character set of ```column``` that it is currently
in. Once set, it decodes it to UTF-8. Upon conversion, the ```column```
 is then converted to UTF-8 string.

