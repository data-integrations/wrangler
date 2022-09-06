# Cheatsheet

| Name                             | Usage                                                                              | Description                                                                                                                                                        |
| -------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
|SWAP|swap &lt;column1&gt; &lt;column2&gt;|Swaps the column names of two columns.|
|ENCODE|encode &lt;base32\|base64\|hex&gt; &lt;column&gt;|Encodes column values using one of base32, base64, or hex.|
|XPATH|xpath &lt;column&gt; &lt;destination&gt; &lt;xpath&gt;|Extract a single XML element or attribute using XPath.|
|GENERATE-UUID|generate-uuid &lt;column&gt;|Populates a column with a universally unique identifier (UUID) of the record.|
|LOWERCASE|lowercase &lt;column&gt;|Changes the column values to lowercase.|
|WRITE-AS-CSV|write-as-csv &lt;column&gt;|Writes the records files as well-formatted CSV|
|PARSE-AS-PROTOBUF|parse-as-protobuf &lt;column&gt; &lt;schema-id&gt; &lt;record-name&gt; [version]|Parses column as protobuf encoded memory representations.|
|HASH|hash &lt;column&gt; &lt;algorithm&gt; [&lt;encode=true\|false&gt;]|Creates a message digest for the column using algorithm, replacing the column value.|
|JSON-PATH|json-path &lt;source&gt; &lt;destination&gt; &lt;json-path-expression&gt;|Parses JSON elements using a DSL (a JSON path expression).|
|MASK-NUMBER|mask-number &lt;column&gt; &lt;pattern&gt;|Masks a column value using the specified masking pattern.|
|TEXT-DISTANCE|text-distance &lt;method&gt; &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;|Calculates a text distance measure between two columns containing string.|
|PARSE-XML-TO-JSON|parse-xml-to-json &lt;column&gt; [&lt;depth&gt;]|Parses a XML document to JSON representation.|
|PARSE-AS-HL7|parse-as-hl7 &lt;column&gt; [&lt;depth&gt;]|Parses &lt;column&gt; for Health Level 7 Version 2 (HL7 V2) messages; &lt;depth&gt; indicates at which point JSON object enumeration terminates.|
|FIND-AND-REPLACE|find-and-replace &lt;column&gt; &lt;sed-expression&gt;|Finds and replaces text in column values using a sed-format expression.|
|RENAME|rename &lt;old&gt; &lt;new&gt;|Renames an existing column.|
|PARSE-AS-AVRO|parse-as-avro &lt;column&gt; &lt;schema-id&gt; &lt;json\|binary&gt; [version]|Parses column as AVRO generic record.|
|FILL-NULL-OR-EMPTY|fill-null-or-empty &lt;column&gt; &lt;fixed-value&gt;|Fills a value of a column with a fixed value if it is either null or empty.|
|SET-TYPE|set-type &lt;column&gt; &lt;type&gt;|Converting data type of a column.|
|RTRIM|rtrim &lt;column&gt;|Trimming whitespace from right side of a string.|
|INVOKE-HTTP|invoke-http &lt;url&gt; &lt;column&gt;[,&lt;column&gt;*] &lt;header&gt;[,&lt;header&gt;*]|[EXPERIMENTAL] Invokes an HTTP endpoint, passing columns as a JSON map (potentially slow).|
|COLUMNS-REPLACE|columns-replace &lt;sed-expression&gt;|Modifies column names in bulk using a sed-format expression.|
|SEND-TO-ERROR|send-to-error &lt;condition&gt;|Send records that match condition to the error collector.|
|SET-RECORD-DELIM|set-record-delim &lt;column&gt; &lt;delimiter&gt; [&lt;limit&gt;]|Sets the record delimiter.|
|SET-VARIABLE|set-variable &lt;variable&gt; &lt;expression&gt;|Sets the value for a transient variable for the record being processed.|
|SET-CHARSET|set-charset &lt;column&gt; &lt;charset&gt;|Sets the character set decoding to UTF-8.|
|WRITE-AS-JSON-OBJECT|write-as-json-object &lt;dest-column&gt; [&lt;src-column&gt;[,&lt;src-column&gt;]|Creates a JSON object based on source columns specified. JSON object is written into dest-column.|
|KEEP|keep &lt;column&gt;[,&lt;column&gt;*]|Keeps the specified columns and drops all others.|
|CUT-CHARACTER|cut-character &lt;source&gt; &lt;destination&gt; &lt;type&gt; &lt;range\|indexes&gt;|UNIX-like 'cut' directive for splitting text.|
|SPLIT-TO-ROWS|split-to-rows &lt;column&gt; &lt;separator&gt;|Splits a column into multiple rows, copies the rest of the columns.|
|XPATH-ARRAY|xpath-array &lt;column&gt; &lt;destination&gt; &lt;xpath&gt;|Extract XML element or attributes as JSON array using XPath.|
|FAIL|fail &lt;condition&gt;|Fails when the condition is evaluated to true.|
|INCREMENT-VARIABLE|increment-variable &lt;variable&gt; &lt;value&gt; &lt;expression&gt;|Wrangler - A interactive tool for data cleansing and transformation.|
|PARSE-AS-XML|parse-as-xml &lt;column&gt;|Parses a column as XML.|
|PARSE-AS-FIXED-LENGTH|parse-as-fixed-length &lt;column&gt; &lt;width&gt;[,&lt;width&gt;*] [&lt;padding-character&gt;]|Parses fixed-length records using the specified widths and padding-character.|
|CHANGE-COLUMN-CASE|change-column-case lower\|upper|Changes the case of column names to either lowercase or uppercase.|
|SPLIT-EMAIL|split-email &lt;column&gt;|Split a email into account and domain.|
|URL-ENCODE|url-encode &lt;column&gt;|URL encode a column value.|
|WRITE-AS-JSON-MAP|write-as-json-map &lt;column&gt;|Writes all record columns as JSON map.|
|MASK-SHUFFLE|mask-shuffle &lt;column&gt;|Masks a column value by shuffling characters while maintaining the same length.|
|DROP|drop &lt;column&gt;[,&lt;column&gt;*]|Drop one or more columns.|
|DECODE|decode &lt;base32\|base64\|hex&gt; &lt;column&gt;|Decodes column values using one of base32, base64, or hex.|
|SPLIT|split &lt;source&gt; &lt;delimiter&gt; &lt;new-column-1&gt; &lt;new-column-2&gt;|[DEPRECATED] Use 'split-to-columns' or 'split-to-rows'.|
|PARSE-AS-SIMPLE-DATE|parse-as-simple-date &lt;column&gt; &lt;format&gt;|Parses a column as date using format.|
|DIFF-DATE|diff-date &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;|Calculates the difference in milliseconds between two Date objects.Positive if &lt;column2&gt; earlier. Must use 'parse-as-date' or 'parse-as-simple-date' first.|
|INDEXSPLIT|indexsplit &lt;source&gt; &lt;start&gt; &lt;end&gt; &lt;destination&gt;|[DEPRECATED] Use the 'split-to-columns' or 'parse-as-fixed-length' directives instead.|
|PARSE-AS-AVRO-FILE|parse-as-avro-file &lt;column&gt;|parse-as-avro-file &lt;column&gt;.|
|FILTER-ROW-IF-TRUE|filter-row-if-true &lt;condition&gt;|[DEPRECATED] Filters rows if condition is evaluated to true. Use 'filter-rows-on' instead.|
|SPLIT-URL|split-url &lt;column&gt;|Split a url into it's components host,protocol,port,etc.|
|FORMAT-DATE|format-date &lt;column&gt; &lt;format&gt;|Formats a column using a date-time format. Use 'parse-as-date` beforehand.|
|QUANTIZE|quantize &lt;source&gt; &lt;destination&gt; &lt;[range1:range2)=value&gt;,[&lt;range1:range2=value&gt;]*|Quanitize the range of numbers into label values.|
|PARSE-AS-EXCEL|parse-as-excel &lt;column&gt; [&lt;sheet number \| sheet name&gt;]|Parses column as Excel file.|
|PARSE-AS-DATE|parse-as-date &lt;column&gt; [&lt;timezone&gt;]|Parses column values as dates using natural language processing and automatically identifying the format (expensive in terms of time consumed).|
|TABLE-LOOKUP|table-lookup &lt;column&gt; &lt;table&gt;|Uses the given column as a key to perform a lookup into the specified table.|
|FILTER-ROWS-ON|filter-rows-on empty-or-null-columns &lt;column&gt;[,&lt;column&gt;*]|Filters row that have empty or null columns.|
|TRIM|trim &lt;column&gt;|Trimming whitespace from both sides of a string.|
|URL-DECODE|url-decode &lt;column&gt;|URL decode a column value.|
|FLATTEN|flatten &lt;column&gt;[,&lt;column&gt;*]|Separates array elements of one or more columns into indvidual records, copying the other columns.|
|UPPERCASE|uppercase &lt;column&gt;|Changes the column values to uppercase.|
|CATALOG-LOOKUP|catalog-lookup &lt;catalog&gt; &lt;column&gt;|Looks-up values from pre-loaded (static) catalogs.|
|PARSE-AS-LOG|parse-as-log &lt;column&gt; &lt;format&gt;|Parses Apache HTTPD and NGINX logs.|
|LTRIM|ltrim &lt;column&gt;|Trimming whitespace from left side of a string.|
|EXTRACT-REGEX-GROUPS|extract-regex-groups &lt;column&gt; &lt;regex-with-groups&gt;|Extracts data from a regex group into its own column.|
|PARSE-AS-CSV|parse-as-csv &lt;column&gt; &lt;delimiter&gt; [&lt;header=true\|false&gt;]|Parses a column as CSV (comma-separated values).|
|FILTER-ROW-IF-MATCHED|filter-row-if-matched &lt;column&gt; &lt;regex&gt;|[DEPRECATED] Filters rows if the regex is matched. Use 'filter-rows-on' instead.|
|PARSE-AS-JSON|parse-as-json &lt;column&gt; [&lt;depth&gt;]|Parses a column as JSON.|
|SET COLUMN|set column &lt;column&gt; &lt;jexl-expression&gt;|Sets a column by evaluating a JEXL expression.|
|STEMMING|stemming &lt;column&gt;|Apply Porter Stemming on the column value.|
|COPY|copy &lt;source&gt; &lt;destination&gt; [&lt;force=true\|false&gt;]|Copies values from a source column into a destination column.|
|SET-COLUMN|set-column &lt;column&gt; &lt;expression&gt;|Sets a column the result of expression execution.|
|SPLIT-TO-COLUMNS|split-to-columns &lt;column&gt; &lt;regex&gt;|Splits a column into one or more columns around matches of the specified regular expression.|
|CLEANSE-COLUMN-NAME|cleanse-column-names|Sanatizes column names: trims, lowercases, and replaces all but [A-Z][a-z][0-9]_.with an underscore '_'.|
|SET COLUMNS|set columns &lt;columm&gt;[,&lt;column&gt;*]|Sets the name of columns, in the order they are specified.|
|TITLECASE|titlecase &lt;column&gt;|Changes the column values to title case.|
|MERGE|merge &lt;column1&gt; &lt;column2&gt; &lt;new-column&gt; &lt;separator&gt;|Merges values from two columns using a separator into a new column.|
|TEXT-METRIC|text-metric &lt;method&gt; &lt;column1&gt; &lt;column2&gt; &lt;destination&gt;|Calculates the metric for comparing two string values.|
|SET FORMAT|set format csv &lt;delimiter&gt; &lt;skip empty lines&gt;|[DEPRECATED] Parses the predefined column as CSV. Use 'parse-as-csv' instead.|
|FORMAT-UNIX-TIMESTAMP|format-unix-timestamp &lt;column&gt; &lt;format&gt;|Formats a UNIX timestamp using the specified format|
|FILTER-ROW-IF-NOT-MATCHED|filter-row-if-not-matched &lt;column&gt; &lt;regex&gt;|Filters rows if the regex does not match|
|FILTER-ROW-IF-FALSE|filter-row-if-false &lt;condition&gt;|Filters rows if the condition evaluates to false|
