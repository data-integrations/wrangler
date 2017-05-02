# Parse as XML

The `parse-as-xml` directive parses an XML document. The directive operates on a column to
parse that into an XML VTD object; that can be further queried using the
[xpath](xpath.md), or [xpath-array](xpath.md), [xpath-attr](xpath-attr.md), or
[xpath-array-attr](xpath-attr.md) directives.


## Syntax
```
parse-as-xml <column>
```

The `<column>` is the name of the column in the record that is an XML document.


## Usage Notes

The `parse-as-xml` directive efficiently parses and represents an XML document into an
in-memory structure that can then be queried using other directives.
