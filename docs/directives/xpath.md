# XPath

XPATH and XPATH-ARRAY allows you navigate through the XML elements and attributes in a XML document.

## Syntax
```
  xpath <column> <destination> <xpath>
  xpath-array <column> <destination> <xpath>
```

```column``` is source for navigating with XPath.


## Usage Notes

This column should be first applied "PARSE-AS-XML" directive before applying this XPATH directive. Applying
"PARSE-AS-XML" will generate [VTDNav](http://vtd-xml.sourceforge.net/javadoc/com/ximpleware/VTDNav.html) Object that
you can then apply XPATH directive on. VTD Nav is the most memory efficient representation random-access XML parser.

```destination``` specifies the column name for the result to be stored from the XPath navigation. If the XPath does
not yield a value, then 'null' value is written to the column.

XPATH-ARRAY directive should be used when the XPath could result in multiple values. The resulting ```destination```
value is a JSON array of all the matches for the XPath.

