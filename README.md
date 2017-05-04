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

Check the documentation of the Wrangler Transform [here](transform/docs/Wrangler-transform.md).


## Demo Videos and Recipes

* Videos  
  * [SCREENCAST] [Building from the GitHub source](https://youtu.be/pGGjKU04Y38) 
  * [VOICE-OVER] [End-to-End Demo Video](https://youtu.be/AnhF0qRmn24)
  * [SCREENCAST] [Ingesting into Kudu](https://www.youtube.com/watch?v=KBW7a38vlUM)
  * [SCREENCAST] [Realtime HL7 CCDA XML from Kafka into Time Parititioned Parquet](https://youtu.be/0fqNmnOnD-0)
  * [SCREENCAST] [Parsing JSON file](https://youtu.be/vwnctcGDflE)
  * [SCREENCAST] [Flattening arrays](https://youtu.be/SemHxgBYIsY)
  * [SCREENCAST] [Data cleansing with send-to-error directive](https://www.youtube.com/watch?v=aZd5H8hIjDc)
  * [SCREENCAST] [Publishing to Kafka](https://www.youtube.com/watch?v=xdc8pvvlI48) 
  * [SCREENCAST] [Fixed length to JSON](https://www.youtube.com/watch?v=3AXu4m1swuM)  
  
* Recipes
  * [Log Parsing](demos/Parsing-Apache-Log.md)
  * [HL7 CCDA XML Parsing](demos/HL7-CCDA-XML-Parsing.md)
  * [CSV Parsing and Extracting Column Values](demos/CSV-Parsing-And-Extraction.md)


## Concepts

This implementation of Wrangler uses these concepts.

### Record

A *Record* is a collection of field names and field values.

### Column

A *Column* is a data value of any of the supported Java types, one for each record.

### Directive

A *Directive* is a single data manipulation instruction, specified to either transform,
filter, or pivot a single record into zero or more records. A directive can generate one
or more *steps* to be executed by a Pipeline.

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

These are the different directives currently available:

* **Parsers**
  * [CSV Parser](docs/directives/csv-parser.md)
  * [JSON Parser](docs/directives/parse-as-json.md)
  * [JSON Path](docs/directives/json-path.md)
  * [XML Parser](docs/directives/parse-as-xml.md)
  * [XPath](docs/directives/xpath.md)
  * [XML To JSON](docs/directives/parse-xml-to-json.md)
  * [Fixed Length Parser](docs/directives/fixed-length-parser.md)
  * [HTTPD and NGNIX Log Parser](docs/directives/parse-as-log.md)
  * [Date Parser](docs/directives/parse-as-date.md)
  * [Simple Date Parser](docs/directives/parse-as-simple-date.md)
  * [HL7 Parser](docs/directives/parse-as-hl7.md)
* **Output Formatters**
  * [JSON Map Formatter](docs/directives/write-as-json-map.md)
  * [CSV Formatter](docs/directives/write-as-csv.md)
* **Transformations**
  * [Change Text Case](docs/directives/change-case.md)
  * [Index Split](docs/directives/index-split.md)
  * [Split by Separator](docs/directives/split-by-separator.md)
  * [Find and Replace](docs/directives/find-and-replace.md)
  * [Cut](docs/directives/cut.md)
  * [Expressions](docs/directives/expression.md)
  * [URL Encode](docs/directives/url-encode.md)
  * [URL Decode](docs/directives/url-decode.md)
  * [Split Email Address](docs/directives/split-email.md)
  * [Split URL](docs/directives/split-url.md)
  * [Fuzzy String Match - Distance](docs/directives/text-distance.md)
  * [Fuzzy String Match - Metric](docs/directives/text-metric.md)
  * [Quantization](docs/directives/quantize.md)
  * [Regex Group Extractor](docs/directives/extract-regex-groups.md)
  * [Setting Character Set](docs/directives/set-charset.md)
  * [Setting Record Delimiter](docs/directives/set-record-delim.md)
  * [Experimental : Invoke HTTP Service](docs/directives/invoke-http.md)
* **Encoders and Decoders**
  * [Encode as Base-32/64 or Hex](docs/directives/encode.md)
  * [Decode Base-32/64 or Hex](docs/directives/decode.md)
* **Unique ID**
  * [UUID Generation](docs/directives/generate-uuid.md)
* **Date Transformations**
  * [Format Date](docs/directives/format-date.md)
  * [Format Unix Timestamp](docs/directives/format-timestamp.md)
  * [Diff Date](docs/directives/diff-date.md)
* **Lookups**
  * [Static Catalog Lookup - ICD-9 Code](docs/directives/catalog-lookup.md)
  * [Static Catalog Lookup - ICD-10 Code - 2016, 2017](docs/directives/catalog-lookup.md)
  * [Table Lookup](docs/directives/table-lookup.md)
* **Hashing & Masking**
  * [Substitution Masking](docs/directives/mask-substitution.md)
  * [Number Masking](docs/directives/mask-number.md)
  * [Message Digest or Hash](docs/directives/hash.md)
* **Row Operations**
  * [Flatten](docs/directives/flatten.md)
  * [Send to Error](docs/directives/send-to-error.md)
  * [Split To Rows](docs/directives/split-to-rows.md)
  * [Filter Rows On](docs/directives/filter-rows-on.md)
  * [Filter Row using Regex](docs/directives/filter-row-if-matched.md) (_Deprecated_)
  * [Filter Row on Condition](docs/directives/filter-row-if-true.md) (_Deprecated_)
* **Column Operations**
  * [Drop Column](docs/directives/drop.md)
  * [Rename Column](docs/directives/rename.md)
  * [Copy Column](docs/directives/copy.md)
  * [Merge Columns](docs/directives/merge.md)
  * [Keep Columns](docs/directives/keep.md)
  * [Swap Columns](docs/directives/swap.md)
  * [Split To Columns](docs/directives/split-to-columns.md)
  * [Fill Null or Empty Columns](docs/directives/fill-null-or-empty.md)
  * [Replace Column Names - Bulk](docs/directives/columns-replace.md)
  * [Sanatize Column Names](docs/directives/cleanse-column-names.md)
  * [Set Column Names](docs/directives/set-columns.md)
* **NLP**
  * [Stemming Tokenized Words](docs/directives/stemming.md)
* **Functions**
    * [JSON](docs/functions/JSON.md)
    * [Types](docs/functions/Types.md)


## Performance

Initial performance tests show that with a set of directives of medium complexity for
transforming data, *DataPrep* is able to process at about 60K records per second. The
rates below are specified as *records/second*. Additional details and test results
[are available](docs/performance.md).

| Directive Complexity | Column Count| Records | Size | Mean Rate | 1 Minute Rate | 5 Minute Rate | 15 Minute Rate |
|----------------------|-------------|---------|------|-----------|---------------|---------------|----------------|
| Medium | 18 | 13,499,973 | 4,499,534,313 | 64,998.50 | 64,921.29 | 46,866.70 | 36,149.86 |
| Medium | 18 | 80,999,838 | 26,997,205,878 | 62,465.93 | 62,706.39 | 60,755.41 | 56,673.32 |


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

These are the different services provided:

* [Administration and Management](docs/service/admin.md)
* [Directive Execution](docs/service/execution.md)
* [Column Type Detection and Statistics](docs/service/statistics.md)
* [Column Name Validation](docs/service/validation.md)


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
