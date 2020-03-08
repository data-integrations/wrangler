# Directives

These directives are currently available:

| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **Parsers**                                                            |                                                                  |
| [JSON Path](wrangler-docs/directives/json-path.md)                              | Uses a DSL (a JSON path expression) for parsing JSON records     |
| [Parse as AVRO](wrangler-docs/directives/parse-as-avro.md)                      | Parsing an AVRO encoded message - either as binary or json       |
| [Parse as AVRO File](wrangler-docs/directives/parse-as-avro-file.md)            | Parsing an AVRO data file                                        |
| [Parse as CSV](wrangler-docs/directives/parse-as-csv.md)                        | Parsing an input record as comma-separated values                |
| [Parse as Date](wrangler-docs/directives/parse-as-date.md)                      | Parsing dates using natural language processing                  |
| [Parse as Excel](wrangler-docs/directives/parse-as-excel.md)                    | Parsing excel file.                                              |
| [Parse as Fixed Length](wrangler-docs/directives/parse-as-fixed-length.md)      | Parses as a fixed length record with specified widths            |
| [Parse as HL7](wrangler-docs/directives/parse-as-hl7.md)                        | Parsing Health Level 7 Version 2 (HL7 V2) messages               |
| [Parse as JSON](wrangler-docs/directives/parse-as-json.md)                      | Parsing a JSON object                                            |
| [Parse as Log](wrangler-docs/directives/parse-as-log.md)                        | Parses access log files as from Apache HTTPD and nginx servers   |
| [Parse as Protobuf](wrangler-docs/directives/parse-as-log.md)                   | Parses an Protobuf encoded in-memory message using descriptor    |
| [Parse as Simple Date](wrangler-docs/directives/parse-as-simple-date.md)        | Parses date strings                                              |
| [Parse XML To JSON](wrangler-docs/directives/parse-xml-to-json.md)              | Parses an XML document into a JSON structure                     |
| [Parse as Currency](wrangler-docs/directives/parse-as-currency.md)              | Parses a string representation of currency into a number.        |
| **Output Formatters**                                                  |                                                                  |
| [Write as CSV](wrangler-docs/directives/write-as-csv.md)                        | Converts a record into CSV format                                |
| [Write as JSON](wrangler-docs/directives/write-as-json-map.md)                  | Converts the record into a JSON map                              |
| [Write JSON Object](wrangler-docs/directives/write-as-json-object.md)           | Composes a JSON object based on the fields specified.            |
| [Format as Currency](wrangler-docs/directives/format-as-currency.md)            | Formats a number as currency as specified by locale.             |
| **Transformations**                                                    |                                                                  |
| [Changing Case](wrangler-docs/directives/changing-case.md)                      | Changes the case of column values                                |
| [Cut Character](wrangler-docs/directives/cut-character.md)                      | Selects parts of a string value                                  |
| [Set Column](wrangler-docs/directives/set-column.md)                            | Sets the column value to the result of an expression execution   |
| [Find and Replace](wrangler-docs/directives/find-and-replace.md)                | Transforms string column values using a "sed"-like expression    |
| [Index Split](wrangler-docs/directives/index-split.md)                          | (_Deprecated_)                                                   |
| [Invoke HTTP](wrangler-docs/directives/invoke-http.md)                          | Invokes an HTTP Service (_Experimental_, potentially slow)       |
| [Quantization](wrangler-docs/directives/quantize.md)                            | Quantizes a column based on specified ranges                     |
| [Regex Group Extractor](wrangler-docs/directives/extract-regex-groups.md)       | Extracts the data from a regex group into its own column         |
| [Setting Character Set](wrangler-docs/directives/set-charset.md)                | Sets the encoding and then converts the data to a UTF-8 String   |
| [Setting Record Delimiter](wrangler-docs/directives/set-record-delim.md)        | Sets the record delimiter                                        |
| [Split by Separator](wrangler-docs/directives/split-by-separator.md)            | Splits a column based on a separator into two columns            |
| [Split Email Address](wrangler-docs/directives/split-email.md)                  | Splits an email ID into an account and its domain                |
| [Split URL](wrangler-docs/directives/split-url.md)                              | Splits a URL into its constituents                               |
| [Text Distance (Fuzzy String Match)](wrangler-docs/directives/text-distance.md) | Measures the difference between two sequences of characters      |
| [Text Metric (Fuzzy String Match)](wrangler-docs/directives/text-metric.md)     | Measures the difference between two sequences of characters      |
| [URL Decode](wrangler-docs/directives/url-decode.md)                            | Decodes from the `application/x-www-form-urlencoded` MIME format |
| [URL Encode](wrangler-docs/directives/url-encode.md)                            | Encodes to the `application/x-www-form-urlencoded` MIME format   |
| [Trim](wrangler-docs/directives/trim.md)                                        | Functions for trimming white spaces around string data           |
| **Encoders and Decoders**                                              |                                                                  |
| [Decode](wrangler-docs/directives/decode.md)                                    | Decodes a column value as one of `base32`, `base64`, or `hex`    |
| [Encode](wrangler-docs/directives/encode.md)                                    | Encodes a column value as one of `base32`, `base64`, or `hex`    |
| **Unique ID**                                                          |                                                                  |
| [UUID Generation](wrangler-docs/directives/generate-uuid.md)                    | Generates a universally unique identifier (UUID)                 |
| **Date Transformations**                                               |                                                                  |
| [Diff Date](wrangler-docs/directives/diff-date.md)                              | Calculates the difference between two dates                      |
| [Format Date](wrangler-docs/directives/format-date.md)                          | Custom patterns for date-time formatting                         |
| [Format Unix Timestamp](wrangler-docs/directives/format-unix-timestamp.md)      | Formats a UNIX timestamp as a date                               |
| **Lookups**                                                            |                                                                  |
| [Catalog Lookup](wrangler-docs/directives/catalog-lookup.md)                    | Static catalog lookup of ICD-9, ICD-10-2016, ICD-10-2017 codes   |
| [Table Lookup](wrangler-docs/directives/table-lookup.md)                        | Performs lookups into Table datasets                             |
| **Hashing & Masking**                                                  |                                                                  |
| [Message Digest or Hash](wrangler-docs/directives/hash.md)                      | Generates a message digest                                       |
| [Mask Number](wrangler-docs/directives/mask-number.md)                          | Applies substitution masking on the column values                |
| [Mask Shuffle](wrangler-docs/directives/mask-shuffle.md)                        | Applies shuffle masking on the column values                     |
| **Row Operations**                                                     |                                                                  |
| [Filter Row if Matched](wrangler-docs/directives/filter-row-if-matched.md)      | Filters rows that match a pattern for a column                                         |
| [Filter Row if True](wrangler-docs/directives/filter-row-if-true.md)            | Filters rows if the condition is true.                                                  |
| [Filter Row Empty of Null](wrangler-docs/directives/filter-empty-or-null.md)    | Filters rows that are empty of null.                    |
| [Flatten](wrangler-docs/directives/flatten.md)                                  | Separates the elements in a repeated field                       |
| [Fail on condition](wrangler-docs/directives/fail.md)                           | Fails processing when the condition is evaluated to true.        |
| [Send to Error](wrangler-docs/directives/send-to-error.md)                      | Filtering of records to an error collector                       |
| [Send to Error And Continue](wrangler-docs/directives/send-to-error-and-continue.md) | Filtering of records to an error collector and continues processing                      |
| [Split to Rows](wrangler-docs/directives/split-to-rows.md)                      | Splits based on a separator into multiple records                |
| **Column Operations**                                                  |                                                                  |
| [Change Column Case](wrangler-docs/directives/change-column-case.md)            | Changes column names to either lowercase or uppercase            |
| [Changing Case](wrangler-docs/directives/changing-case.md)                      | Change the case of column values                                 |
| [Cleanse Column Names](wrangler-docs/directives/cleanse-column-names.md)        | Sanatizes column names, following specific rules                 |
| [Columns Replace](wrangler-docs/directives/columns-replace.md)                  | Alters column names in bulk                                      |
| [Copy](wrangler-docs/directives/copy.md)                                        | Copies values from a source column into a destination column     |
| [Drop Column](wrangler-docs/directives/drop.md)                                 | Drops a column in a record                                       |
| [Fill Null or Empty Columns](wrangler-docs/directives/fill-null-or-empty.md)    | Fills column value with a fixed value if null or empty           |
| [Keep Columns](wrangler-docs/directives/keep.md)                                | Keeps specified columns from the record                          |
| [Merge Columns](wrangler-docs/directives/merge.md)                              | Merges two columns by inserting a third column                   |
| [Rename Column](wrangler-docs/directives/rename.md)                             | Renames an existing column in the record                         |
| [Set Column Header](wrangler-docs/directives/set-headers.md)                     | Sets the names of columns, in the order they are specified       |
| [Split to Columns](wrangler-docs/directives/split-to-columns.md)                | Splits a column based on a separator into multiple columns       |
| [Swap Columns](wrangler-docs/directives/swap.md)                                | Swaps column names of two columns                                |
| [Set Column Data Type](wrangler-docs/directives/set-type.md)                    | Convert data type of a column                                    |
| **NLP**                                                                |                                                                  |
| [Stemming Tokenized Words](wrangler-docs/directives/stemming.md)                | Applies the Porter stemmer algorithm for English words           |
| **Transient Aggregators & Setters**                                    |                                                                  |
| [Increment Variable](wrangler-docs/directives/increment-variable.md)            | Increments a transient variable with a record of processing.     |
| [Set Variable](wrangler-docs/directives/set-variable.md)                        | Sets a transient variable with a record of processing.     |
