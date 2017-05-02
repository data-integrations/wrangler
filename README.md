# Data Prep

![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)
![cdap-transform](https://cdap-users.herokuapp.com/assets/cdap-transform.svg)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com)
[![Build Status](https://travis-ci.org/hydrator/wrangler.svg?branch=develop)](https://travis-ci.org/hydrator/wrangler)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/11434/badge.svg)](https://scan.coverity.com/projects/hydrator-wrangler-transform)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A collection of libraries, a pipeline plugin, and a CDAP service for performing data
cleansing, transformation, and filtering using a set of data manipulation instructions
(directives). These instructions are either generated using an interative visual tool or
are manually created.

The Data Prep Transform is [separately documented](transform/docs/data-prep-transform.md).


## Demo Videos and Recipes

* Videos
  * [SCREENCAST] [Building Data Prep from the GitHub source](https://youtu.be/pGGjKU04Y38)
  * [VOICE-OVER] [End-to-End Demo Video](https://youtu.be/AnhF0qRmn24)
  * [SCREENCAST] [Ingesting into Kudu](https://www.youtube.com/watch?v=KBW7a38vlUM)
  * [SCREENCAST] [Realtime HL7 CCDA XML from Kafka into Time Parititioned Parquet](https://youtu.be/0fqNmnOnD-0)
  * [SCREENCAST] [Parsing JSON File](https://youtu.be/vwnctcGDflE)
  * [SCREENCAST] [Flattening Arrays](https://youtu.be/SemHxgBYIsY)
* Recipes
  * [Parsing Apache Log Files](demos/parsing-apache-log-files.md)
  * [Parsing CSV Files and Extracting Column Values](demos/parsing-csv-extracting-column-values.md)
  * [Parsing HL7 CCDA XML Files](demos/parsing-hl7-ccda-xml-files.md)


## Concepts

This implementation of Data Prep uses the concepts of _Record_, _Column_, _Directive_,
_Step_, and _Pipeline_.

### Record

A *Record* is a collection of field names and field values.

### Column

A *Column* is a data value of any of the supported Java types, one for each record.

### Directive

A *Directive* is a single data manipulation instruction, specified to either transform,
filter, or pivot a single record into zero or more records. A directive can generate one
or more *steps* to be executed by a pipeline.

### Step

A *Step* is an implementation of a data transformation function, operating on a single
record or set of records. A step can generate zero or more records from the application of
a function.

### Pipeline

A *Pipeline* is a collection of steps to be applied on a record. The record(s) outputed
from a step are passed to the next step in the pipeline.


## Notations

### Directives

A directive can be represented in text in this format:

```
<command> <argument-1> <argument-2> ... <argument-n>
```

### Record

A record in this documentation will be shown as a JSON object with an object key
representing the column names and a value shown by the plain representation of the
the data, without any mention of types.

For example:

```
{
  "id": 1,
  "fname": "root",
  "lname": "joltie",
  "address": {
    "housenumber": "678",
    "street": "Mars Street",
    "city": "Marcity",
    "state": "Maregon",
    "country": "Mari"
  },
  "gender": "M"
}
```


## Available Directives

These directives are currently available:

| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **Parsers**                                                            |                                                                  |
| [JSON Path](docs/directives/json-path.md)                              | Uses a DSL for reading JSON records                              |
| [Parse as CSV](docs/directives/parse-as-csv.md)                        | Parsing an input record as comma-separated values                |
| [Parse as Date](docs/directives/parse-as-date.md)                      | Parsing dates using natural language processing                  |
| [Parse as Fixed Length](docs/directives/parse-as-fixed-length.md)      | Parses as a fixed length record with specified widths            |
| [Parse as HL7](docs/directives/parse-as-hl7.md)                        | Parsing Health Level 7 Version 2 (HL7 V2) messages               |
| [Parse as JSON](docs/directives/parse-as-json.md)                      | Parsing a JSON object                                            |
| [Parse as Log](docs/directives/parse-as-log.md)                        | Parses access log files as from Apache HTTPD and nginx servers   |
| [Parse as Simple Date](docs/directives/parse-as-simple-date.md)        | Parses date strings                                              |
| [Parse as XML](docs/directives/parse-as-xml.md)                        | Parses an XML document                                           |
| [Parse XML To JSON](docs/directives/parse-xml-to-json.md)              | Parses an XML document into a JSON structure                     |
| [XPath](docs/directives/xpath.md)                                      | Navigate the XML elements and attributes of an XML document      |
| **Output Formatters**                                                  |                                                                  |
| [Write as CSV](docs/directives/write-as-csv.md)                        | Converts a record into CSV format                                |
| [Write as JSON](docs/directives/write-as-json-map.md)                  | Converts the record into a JSON map                              |
| **Transformations**                                                    |                                                                  |
| [Changing Case](docs/directives/changing-case.md)                      | Changes the case of column values                                |
| [Cut](docs/directives/cut.md)                                          | Selects parts of a string value                                  |
| [Set Column](docs/directives/set-column.md)                            | Sets the column value to the result of an expression execution   |
| [Find and Replace](docs/directives/find-and-replace.md)                | Transforms string column values using a "sed"-like expression    |
| [Index Split](docs/directives/index-split.md)                          | (_Deprecated_)                                                   |
| [Quantization](docs/directives/quantize.md)                            | Quantizes a column based on specified ranges                     |
| [Regex Group Extractor](docs/directives/extract-regex-groups.md)       | Extracts the data from a regex group into its own column         |
| [Setting Character Set](docs/directives/set-charset.md)                | Sets the encoding and then converts the data to a UTF-8 String   |
| [Setting Record Delimiter](docs/directives/set-record-delim.md)        | Sets the record delimiter                                        |
| [Split by Separator](docs/directives/split-by-separator.md)            | Splits a column based on a separator into two columns            |
| [Split Email Address](docs/directives/split-email.md)                  | Splits an email ID into an account and its domain                |
| [Split URL](docs/directives/split-url.md)                              | Splits a URL into its constituents                               |
| [Text Distance (Fuzzy String Match)](docs/directives/text-distance.md) | Measures the difference between two sequences of characters      |
| [Text Metric (Fuzzy String Match)](docs/directives/text-metric.md)     | Measures the difference between two sequences of characters      |
| [URL Decode](docs/directives/url-decode.md)                            | Decodes from the `application/x-www-form-urlencoded` MIME format |
| [URL Encode](docs/directives/url-encode.md)                            | Encodes to the `application/x-www-form-urlencoded` MIME format   |
| **Encoders and Decoders**                                              |                                                                  |
| [Decode](docs/directives/decode.md)                                    | Decodes a column value as one of `base32`, `base64`, or `hex`    |
| [Encode](docs/directives/encode.md)                                    | Encodes a column value as one of `base32`, `base64`, or `hex`    |
| **Unique ID**                                                          |                                                                  |
| [UUID Generation](docs/directives/generate-uuid.md)                    | Generates a universally unique identifier (UUID)                 |
| **Date Transformations**                                               |                                                                  |
| [Diff Date](docs/directives/diff-date.md)                              | Calculates the difference between two dates                      |
| [Format Date](docs/directives/format-date.md)                          | Custom patterns for date-time formatting                         |
| [Format Unix Timestamp](docs/directives/format-unix-timestamp.md)           |                                                                  |
| **Lookups**                                                            |                                                                  |
| [Catalog Lookup](docs/directives/catalog-lookup.md)                    | Static catalog lookup of ICD-9, ICD-10-2016, ICD-10-2017 codes   |
| [Table Lookup](docs/directives/table-lookup.md)                        | Performs lookups into Table datasets                             |
| **Hashing & Masking**                                                  |                                                                  |
| [Message Digest or Hash](docs/directives/hash.md)                      | Generates a message digest                                       |
| [Mask Number](docs/directives/mask-number.md)                          | Applies substitution masking on the column values                |
| [Mask Shuffle](docs/directives/mask-shuffle.md)                        | Applies shuffle masking on the column values                     |
| **Row Operations**                                                     |                                                                  |
| [Filter Row if Matched](docs/directives/filter-row-if-matched.md)      | (_Deprecated_)                                                   |
| [Filter Row if True](docs/directives/filter-row-if-true.md)            | (_Deprecated_)                                                   |
| [Filter Rows On](docs/directives/filter-rows-on.md)                    | Filters records based on a condition                             |
| [Flatten](docs/directives/flatten.md)                                  | Separates the elements in a repeated field                       |
| [Send to Error](docs/directives/send-to-error.md)                      | Filtering of records to an error collector                       |
| [Split To Rows](docs/directives/split-to-rows.md)                      | Splits based on a separator into multiple records                |
| **Column Operations**                                                  |                                                                  |
| [Change Column Case](docs/directives/change-column-case.md)            | Changes column names to either lowercase or uppercase            |
| [Changing Case](docs/directives/changing-case.md)                      | Change the case of column values                                 |
| [Cleanse Column Names](docs/directives/cleanse-column-names.md)        | Sanatizes column names, following specific rules                 |
| [Columns Replace](docs/directives/columns-replace.md)                  | Alters column names in bulk                                      |
| [Copy](docs/directives/copy.md)                                        | Copies values from a source column into a destination column     |
| [Drop Column](docs/directives/drop.md)                                 | Drops a column in a record                                       |
| [Fill Null or Empty Columns](docs/directives/fill-null-or-empty.md)    | Fills column value with a fixed value if null or empty           |
| [Keep Columns](docs/directives/keep.md)                                | Keeps specified columns from the record                          |
| [Merge Columns](docs/directives/merge.md)                              | Merges two columns by inserting a third column                   |
| [Rename Column](docs/directives/rename.md)                             | Renames an existing column in the record                         |
| [Set Column Names](docs/directives/set-columns.md)                     | Sets the names of columns, in the order they are specified       |
| [Split To Columns](docs/directives/split-to-columns.md)                | Splits a column based on a separator into multiple columns       |
| [Swap Columns](docs/directives/swap.md)                                | Swaps column names of two columns                                |
| **NLP**                                                                |                                                                  |
| [Stemming Tokenized Words](docs/directives/stemming.md)                | Applies the Porter stemmer algorithm for English words           |
| **Functions**                                                          |                                                                  |
| [JSON](docs/functions/json-functions.md)                               | Date functions that can be useful in transforming your data      |
| [Types](docs/functions/type-functions.md)                              | Functions for detecting the type of data                         |


## Performance

Initial performance tests show that with a set of directives of medium complexity for
transforming data, *DataPrep* is able to process at about 60K records per second. The
rates below are specified as *records/second*. Additional details and test results
[are available](docs/performance.md).

| Directive Complexity | Column Count | Records    | Size           | Mean Rate | 1 Minute Rate | 5 Minute Rate | 15 Minute Rate |
| -------------------- | -----------: | ---------: | -------------: | --------: | ------------: | ------------: | -------------: |
| Medium               | 18           | 13,499,973 | 4,499,534,313  | 64,998.50 | 64,921.29     | 46,866.70     | 36,149.86      |
| Medium               | 18           | 80,999,838 | 26,997,205,878 | 62,465.93 | 62,706.39     | 60,755.41     | 56,673.32      |


## Data Prep Service

Data Prep is integrated as a CDAP Service to support HTTP RESTful-based interactive
wrangling of data. The main objective of this service is to make it simple and easy to
interactively apply the directives required for parsing a given data set. The service is
not intended to replace full-scale big data processing; it is primarily used to
interactively apply directives on a sample of your data.

The base endpoint is:

```
http://<hostname>:11015/v3/namespaces/<namespace>/apps/dataprep/services/service/methods
```

These services are provided:

* [Administration and Management](docs/service/admin.md)
* [Directive Execution](docs/service/execution.md)
* [Column Type Detection and Statistics](docs/service/statistics.md)
* [Column Name Validation](docs/service/validation.md)

The [Request Format Specification](docs/service/request.md) describes the format that is used for sending
a request to the back-end.


## Building New Directives

As directives are executed as a step, it's a simple three-part process to implement the step and
provide the specifications for a directive.

### Part 1 of 3
In order to add a new step, implement the interface 'Step':

```
/**
 * A interface defining a Data Prep step in a pipeline.
 */
public interface Step {
  /**
   * Executes a Data Prep step on each {@link Row} and returns an array of wrangled {@link Row Rows}.
   *
   * @param records the list of input {@link Record Records} to be wrangled by this step
   * @return the list of wrangled {@link Record Records}
   * @throws StepException if a step exception occurred
   */
  List<Record> execute(List<Record> records) throws StepException;
}
```

### Part 2 of 3
Add a comprehensive test case for testing the directive that has been added.

### Part 3 of 3
Modify the specification to parse the directive specification and create the implementation of
the step you have created in part 1.


## Build
To build your plugin:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the `target` directory for the
`wrangler-transform` and a .jar file for the `wrangler-service` application. These files can be used to
deploy your plugin and the wrangler backend.


## Deployment
You can deploy your plugin using the CDAP CLI:

    > load artifact <target/wrangler-transform-<version>.jar> config-file <target/wrangler-transform-<version>.json>

For example, if your artifact is named `wrangler-transform-1.0.0-SNAPSHOT`:

    > load artifact target/wrangler-transform-1.0.0-SNAPSHOT.jar config-file target/wrangler-transform-1.0.0-SNAPSHOT.json


## Contact

### Mailing Lists

CDAP User Group and Development Discussions:

* [cdap-user@googlegroups.com](https://groups.google.com/d/forum/cdap-user)

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

### IRC Channel

CDAP IRC Channel: [#cdap on irc.freenode.net](http://webchat.freenode.net?channels=%23cdap)

### Slack Team

CDAP Users on Slack: [cdap-users team](https://cdap-users.herokuapp.com)


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
