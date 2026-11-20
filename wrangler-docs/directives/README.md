# Wrangler Directives

This directory contains documentation for all directives available in the Data Prep (Wrangler) framework.
Directives are the building blocks of data transformation recipes in Wrangler.

## Available Directives by Category

### Parsers

- [JSON Path](json-path.md) - Uses a DSL (a JSON path expression) for parsing JSON records
- [Parse as AVRO](parse-as-avro.md) - Parses an AVRO encoded message
- [Parse as AVRO File](parse-as-avro-file.md) - Parses an AVRO data file
- [Parse as CSV](parse-as-csv.md) - Parses a column as comma-separated values
- [Parse as Date](parse-as-date.md) - Parses dates using natural language processing
- [Parse as Excel](parse-as-excel.md) - Parses Excel file
- [Parse as Fixed Length](parse-as-fixed-length.md) - Parses as a fixed length record
- [Parse as HL7](parse-as-hl7.md) - Parses Health Level 7 Version 2 (HL7 V2) messages
- [Parse as JSON](parse-as-json.md) - Parses a JSON object
- [Parse as Log](parse-as-log.md) - Parses Apache HTTPD and nginx logs
- [Parse as Protobuf](parse-as-log.md) - Parses a Protobuf encoded message
- [Parse as Simple Date](parse-as-simple-date.md) - Parses date strings
- [Parse XML To JSON](parse-xml-to-json.md) - Parses an XML document into JSON

### Transformations

- [Aggregate Stats](aggregate-stats.md) - Analyzes byte size and time duration values, generating statistics
- [Changing Case](changing-case.md) - Changes the case of column values
- [Cut Character](cut-character.md) - Selects parts of a string value
- [Set Column](set-column.md) - Sets column value to the result of an expression
- [Find and Replace](find-and-replace.md) - Transforms string column values using a "sed"-like expression

### Column Operations

- [Change Column Case](change-column-case.md) - Changes column names to lowercase or uppercase
- [Cleanse Column Names](cleanse-column-names.md) - Sanitizes column names
- [Columns Replace](columns-replace.md) - Alters column names in bulk
- [Copy](copy.md) - Copies values from one column to another
- [Drop Column](drop.md) - Drops a column from a record
- [Keep Columns](keep.md) - Keeps only specified columns
- [Merge Columns](merge.md) - Merges two columns into a new column
- [Rename Column](rename.md) - Renames a column
- [Set Column Header](set-headers.md) - Sets the names of columns

### Row Operations

- [Filter Row if Matched](filter-row-if-matched.md) - Filters rows matching a pattern
- [Filter Row if True](filter-row-if-true.md) - Filters rows if a condition is true
- [Filter Row Empty or Null](filter-empty-or-null.md) - Filters empty or null rows
- [Flatten](flatten.md) - Separates elements in a repeated field
- [Fail on condition](fail.md) - Fails processing when a condition is true
- [Send to Error](send-to-error.md) - Filters records to an error collector
- [Split to Rows](split-to-rows.md) - Splits a column into multiple records

### Data Type Handling

- [BYTE_SIZE and TIME_DURATION Format Handling](aggregate-stats.md)

## Using Directives

Directives can be used individually or combined into recipes. A recipe is a series of directives that are executed
in order to transform data.

Example recipe:

```rb
parse-as-csv :body ',' true
drop :body
set-column :fullname exp:{ concat(first_name, " ", last_name) }
filter-row-if-true :age < 18
aggregate-stats :file_size :size_stats 'byte'
```

For more information on directive usage, see the [Wrangler Cheatsheet](../cheatsheet.md).