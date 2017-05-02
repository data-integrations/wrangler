# XPath Directives

The `xpath` and `xpath-array` directives navigate the XML elements and attributes of an
XML document.


## Syntax

```
xpath <column> <destination> <xpath>
xpath-array <column> <destination> <xpath>
```

The `<column>` is the source XML to be navigated with an XPath.


## Usage Notes

The source XML `<column>` should first have the [parse-as-xml](parse-as-xml.md) directive
applied before applying either of the XPATH directives.

Applying the `parse-as-xml` directive will generate a
[VTDNav](http://vtd-xml.sourceforge.net/javadoc/com/ximpleware/VTDNav.html) object that
you can then apply an XPATH directive on. VTD Nav is a memory-efficient random-access XML
parser.

The `<destination>` specifies the column name to use for storing the results from the XPath
navigation. If the XPath does not yield a value, then a `null` value is written to the
column.

The `xpath-array` directive should be used when the XPath could result in multiple values.
The resulting `<destination>` value is a JSON array of all the matches for the XPath.
