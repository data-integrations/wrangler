# Parse XML to JSON

The PARSE-XML-TO-JSON directive parses an XML document into a JSON structure. The
directive operates on an input column of type string. Application of this directive
transforms the XML into a JSON document, simplifying further parsing using the
[PARSE-AS-JSON](parse-as-json.md) directive.


## Syntax
```
parse-xml-to-json <column-name> [<depth>] [<keep-strings>]
```

* `<column-name>` is the name of the column in the record that is an XML document.
* `<depth>` indicates the depth at which the XML document parsing should terminate processing.
* `<keep-strings>` An OPTIONAL boolean value that if true, then values will not be coerced into boolean or numeric values and will instead be left as strings. (as per `org.json.XML` rules)
 The default value is `false`


## Usage Notes

The PARSE-XML-TO-JSON directive efficiently parses an XML document and presents it as a
JSON object for further transformation.

The XML document contains elements, attributes, and content text. A sequence of similar
elements is turned into a JSON array, which can then be further parsed using the
[PARSE-AS-JSON](parse-as-json.md) directive.

During parsing, comments, prologs, DTDs, and `<[[ ]]>` notations are ignored.
