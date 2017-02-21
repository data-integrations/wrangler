# Stream Editor (SED)

SED directive performs basic text transformation of column values that are of type string.

## Syntax

```
 sed <column> <sed-script>
```

## Usage Notes

It's a column oriented text processor that operates on a single value in the given column.
The ```sed-script``` is applied on each text to transform the data.

Following is a typical example on how the SED directive is used

```
  sed <column> s/regex/replacement/g
```

Application of this directive will replace the value of the column that matches the ```regex```
with the ```replacement```

The s stands for substitute, while the g stands for global, which means that all matching occurrences
in the line would be replaced. The regular expression (i.e. pattern) to be searched is placed after
the first delimiting symbol (slash here) and the replacement follows the second symbol. Slash (/)
is the conventional symbol, originating in the character for "search".

For example, to replace all occurance of 'hello' to 'world' in the column 'message':

```
  sed message s/hello/world/g
```

The character after the s is the delimiter. It is conventionally a slash. If you want to change a pathname
that contains a slash - say /usr/local/bin to /common/bin - you could use the backslash to quote the slash:

```
  sed 's/\/usr\/local\/bin/\/common\/bin/' <old >new
```


## Example

Let's following is the record

```
  {
    "body" : "one two three four five six seven eight"
  }
```

applying following CUT directive

```
  sed body s/one/ONE/g
  sed body s/two/2/g
```

would result in record as show below

```
  {
    "body" : "One 2 three four five six seven eight",
  }
```

