## Fixed Length Parser

Parses a column as fixed length record with range specifications specified.

### Specification
```
parse-as-fixed-length {column-name} s1-e1[[,[s2]*],[s3-e3]*]*
```

### Example
```
parse-as-fixed-length body 1-2,3-4,5,6-9
```
