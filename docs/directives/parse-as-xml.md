# Parse XML.

PARSE-AS-XML is a directive for parsing an XML document. The directive operates on input column that is of type String.
Application of this directive transforms the XML into JSON document on fly simplifying further parsing using
[PARSE-AS-JSON](parse-as-json.md).

## Syntax 

```
  parse-as-xml <column-name> [<depth>]
```

```column-name``` name of the column in the record that is a XML document.
```depth``` indicates the depth at which XML document should terminate processing.

## Usage Notes

PARSE-AS-XML directive efficently the XML document and presents as JSON object to further transform it.
The XML document contains elements, attributes, and content text. Sequence of similar elements are
turned into JSONArray -- which can then be further parsed using [PARSE-AS-JSON](parse-as-json.md) directive.
During parsing, the comments, prologs, DTDs and <code>&lt;[ [ ]]></code> are ignored.
