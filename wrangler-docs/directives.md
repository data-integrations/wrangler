# Directives

These directives are currently available:

## Parsers

| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [JSON Path](directives/json-path.md)                              | Uses a DSL (a JSON path expression) for parsing JSON records     |
| [Parse as AVRO](directives/parse-as-avro.md)                      | Parsing an AVRO encoded message - either as binary or json       |
| [Parse as AVRO File](directives/parse-as-avro-file.md)            | Parsing an AVRO data file                                        |
| [Parse as CSV](directives/parse-as-csv.md)                        | Parsing an input record as comma-separated values                |
| [Parse as Date](directives/parse-as-date.md)                      | Parsing dates using natural language processing                  |
| [Parse as Excel](directives/parse-as-excel.md)                    | Parsing excel file.                                              |
| [Parse as Fixed Length](directives/parse-as-fixed-length.md)      | Parses as a fixed length record with specified widths            |
| [Parse as HL7](directives/parse-as-hl7.md)                        | Parsing Health Level 7 Version 2 (HL7 V2) messages               |
| [Parse as JSON](directives/parse-as-json.md)                      | Parsing a JSON object                                            |
| [Parse as Log](directives/parse-as-log.md)                        | Parses access log files as from Apache HTTPD and nginx servers   |
| [Parse as Protobuf](directives/parse-as-log.md)                   | Parses an Protobuf encoded in-memory message using descriptor    |
| [Parse as Simple Date](directives/parse-as-simple-date.md)        | Parses date strings                                              |
| [Parse XML To JSON](directives/parse-xml-to-json.md)              | Parses an XML document into a JSON structure                     |
| [Parse as Currency](directives/parse-as-currency.md)              | Parses a string representation of currency into a number.        |

## Output Formatters
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Write as CSV](directives/write-as-csv.md)                        | Converts a record into CSV format                                |
| [Write as JSON](directives/write-as-json-map.md)                  | Converts the record into a JSON map                              |
| [Write JSON Object](directives/write-as-json-object.md)           | Composes a JSON object based on the fields specified.            |
| [Format as Currency](directives/format-as-currency.md)            | Formats a number as currency as specified by locale.             |

## Transformations
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Changing Case](directives/changing-case.md)                      | Changes the case of column values                                |
| [Cut Character](directives/cut-character.md)                      | Selects parts of a string value                                  |
| [Set Column](directives/set-column.md)                            | Sets the column value to the result of an expression execution   |
| [Find and Replace](directives/find-and-replace.md)                | Transforms string column values using a "sed"-like expression    |
| [Index Split](directives/index-split.md)                          | (_Deprecated_)                                                   |
| [Invoke HTTP](directives/invoke-http.md)                          | Invokes an HTTP Service (_Experimental_, potentially slow)       |
| [Quantization](directives/quantize.md)                            | Quantizes a column based on specified ranges                     |
| [Regex Group Extractor](directives/extract-regex-groups.md)       | Extracts the data from a regex group into its own column         |
| [Setting Character Set](directives/set-charset.md)                | Sets the encoding and then converts the data to a UTF-8 String   |
| [Setting Record Delimiter](directives/set-record-delim.md)        | Sets the record delimiter                                        |
| [Split by Separator](directives/split-by-separator.md)            | Splits a column based on a separator into two columns            |
| [Split Email Address](directives/split-email.md)                  | Splits an email ID into an account and its domain                |
| [Split URL](directives/split-url.md)                              | Splits a URL into its constituents                               |
| [Text Distance (Fuzzy String Match)](directives/text-distance.md) | Measures the difference between two sequences of characters      |
| [Text Metric (Fuzzy String Match)](directives/text-metric.md)     | Measures the difference between two sequences of characters      |
| [URL Decode](directives/url-decode.md)                            | Decodes from the `application/x-www-form-urlencoded` MIME format |
| [URL Encode](directives/url-encode.md)                            | Encodes to the `application/x-www-form-urlencoded` MIME format   |
| [Trim](directives/trim.md)                                        | Functions for trimming white spaces around string data           |

## Encoders and Decoders
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Decode](directives/decode.md)                                    | Decodes a column value as one of `base32`, `base64`, or `hex`    |
| [Encode](directives/encode.md)                                    | Encodes a column value as one of `base32`, `base64`, or `hex`    |

## Unique ID
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [UUID Generation](directives/generate-uuid.md)                    | Generates a universally unique identifier (UUID)                 |

## Date Transformations
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Diff Date](directives/diff-date.md)                              | Calculates the difference between two dates                      |
| [Format Date](directives/format-date.md)                          | Custom patterns for date-time formatting                         |
| [Format Unix Timestamp](directives/format-unix-timestamp.md)      | Formats a UNIX timestamp as a date                               |

## Lookups
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Catalog Lookup](directives/catalog-lookup.md)                    | Static catalog lookup of ICD-9, ICD-10-2016, ICD-10-2017 codes   |
| [Table Lookup](directives/table-lookup.md)                        | Performs lookups into Table datasets                             |

## Hashing and Masking
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Message Digest or Hash](directives/hash.md)                      | Generates a message digest                                       |
| [Mask Number](directives/mask-number.md)                          | Applies substitution masking on the column values                |
| [Mask Shuffle](directives/mask-shuffle.md)                        | Applies shuffle masking on the column values                     |

## Row Operations
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Filter Row if Matched](directives/filter-row-if-matched.md)      | Filters rows that match a pattern for a column                                         |
| [Filter Row if True](directives/filter-row-if-true.md)            | Filters rows if the condition is true.                                                  |
| [Filter Row Empty of Null](directives/filter-empty-or-null.md)    | Filters rows that are empty of null.                    |
| [Flatten](directives/flatten.md)                                  | Separates the elements in a repeated field                       |
| [Fail on condition](directives/fail.md)                           | Fails processing when the condition is evaluated to true.        |
| [Send to Error](directives/send-to-error.md)                      | Filtering of records to an error collector                       |
| [Send to Error And Continue](directives/send-to-error-and-continue.md) | Filtering of records to an error collector and continues processing                      |
| [Split to Rows](directives/split-to-rows.md)                      | Splits based on a separator into multiple records                |

## Column Operations
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Change Column Case](directives/change-column-case.md)            | Changes column names to either lowercase or uppercase            |
| [Changing Case](directives/changing-case.md)                      | Change the case of column values                                 |
| [Cleanse Column Names](directives/cleanse-column-names.md)        | Sanatizes column names, following specific rules                 |
| [Columns Replace](directives/columns-replace.md)                  | Alters column names in bulk                                      |
| [Copy](directives/copy.md)                                        | Copies values from a source column into a destination column     |
| [Drop Column](directives/drop.md)                                 | Drops a column in a record                                       |
| [Fill Null or Empty Columns](directives/fill-null-or-empty.md)    | Fills column value with a fixed value if null or empty           |
| [Keep Columns](directives/keep.md)                                | Keeps specified columns from the record                          |
| [Merge Columns](directives/merge.md)                              | Merges two columns by inserting a third column                   |
| [Rename Column](directives/rename.md)                             | Renames an existing column in the record                         |
| [Set Column Header](directives/set-headers.md)                     | Sets the names of columns, in the order they are specified       |
| [Split to Columns](directives/split-to-columns.md)                | Splits a column based on a separator into multiple columns       |
| [Swap Columns](directives/swap.md)                                | Swaps column names of two columns                                |
| [Set Column Data Type](directives/set-type.md)                    | Convert data type of a column                                    |
## Natural Language Processing
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Stemming Tokenized Words](directives/stemming.md)                | Applies the Porter stemmer algorithm for English words           |

## Transient Aggregators & Setters
| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| [Increment Variable](directives/increment-variable.md)            | Increments a transient variable with a record of processing.     |
| [Set Variable](directives/set-variable.md)                        | Sets a transient variable with a record of processing.     |
