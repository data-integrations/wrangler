
# Wrangler Transform

[![Build Status](https://travis-ci.org/hydrator/wrangler-transform.svg?branch=develop)](https://travis-ci.org/hydrator/wrangler-transform)  [![Code Advisor On Demand Status](https://badges.ondemand.coverity.com/jobs/m61o1f34qt265e075g5uk5v4m8)](https://ondemand.coverity.com/jobs/m61o1f34qt265e075g5uk5v4m8/results)

A plugin for performing data transformation based on directives. The directives are generated either by an interactive user interface or manual entered into the plugin. 

## Directives
Wrangler plugin supports an easy way to specify data transformation using directives. Directives are
instructions that tell plugin how to transform the incoming record. All of the directives are transformational
and they operate on the input row to generate a new row. The directives are applied on the input record in
the order they are specified.

## Types of Directives
Following are different types of directives that are supported by the Wrangler plugin.

### Parser

This directive specifies how the input needs to be parsed. Currently Wrangler supports parsing of CSV feed.
The input is parsed as CSV with delimiter specified.

**Specification**
```
  set format {type} {delimiter} {configuration}
```
* type - Currently the only type supported is CSV.
* delimiter - When type is CSV, the delimiter to be used for splitting into columns. If you would like to specify a
delimiter like a tab, then you specify it as '\\t'.
* configuration - Specifies configuration based on type, for CSV, ability to skip empty lines is specifiable.
The value can be either 'true' or 'false'.

**Example**
```
  set format csv , false
```

### Changing Case

Directive that provides the ability to change the case of a column value. One can change the column value
 to uppercase, lowercase or titlecase.

**Specification**
```
 uppercase {column-name}
 lowercase {column-name}
 titlecase {column-name}
```
* column-name - Specifies the name of the column to which the changing case directives are applied.

**Example**
```
  uppercase state
  lowercase email
  titlecase name
```
### Drop a column

Drop a column directive will remove a column from the input record. The resulting output record will not
include the column specified in the directive.

**Specification**
```
  drop {column-name}
```

* column-name - name of the column to be dropped. If the column name doesn't exist, the processing is stopped.

**Example**
```
  drop zipcode
```

### Rename a column

Renames the name of the column.

**Specification**
```
  rename {source-column-name} {destination-column-name}
```
* source-column-name - Name of the column to be renamed. If the column name doesn't exist, the processing is stopped.
* destination-column-name - Name of the column to be set to.

**Example**
```
  rename email emailid
```
### Splitting Column

Often times there is need to split a column based on fixed indexes or based on a delimiter. The Wrangler
plugin support two ways to split a string.

* Based on start and end index &
* Based on delimiter

Index based split will take a source input column value and extract substring from start index to end index into
 a destination column name. This is mainly used for extracting substring from a source string.

```
  indexsplit {source-column-name} {start} {end} {destination-column-name}
```
**Specification**

* source-column-name - Name of the source column that needs to be split
* start - Start index to split. If start is less than 0, then it's defaulted to 0.
* end - End index to split. If end is greater than length of source-column-name value, it's defaulted to it's length.
* destination-column-name - Name of the column into which the value between start,end value from
source-column-name is stored.

**Example**
```
  indexsplit ssn 7 11 last4ssn
```

Delimiter based splitter would split the source column value based on delimiter into two columns.
First column will include the value to the left of the delimiter (excluding delimiter) and the
second column will hold the value to the right of the delimiter.

```
  split {source-column-name} {delimiter} {new-column-1} {new-column-2}
```
**Specification**

* source-column-name - Name of the source column that needs to be split
* delimiter - Delimiter to be used to split the source-column-name
* new-column-1 - Name of the new column that contains the substring left of delimiter. If the column doesn't
exist then it will be added. If it exists, it will replace.
* new-column-2 - Name of the new column that contains the substring right of delimiter. If the column doesn't
exist then it will be added. If it exists, it will replace.

**Example**
```
  split email @ name domain
```

### Specify column names

This directive specifies the name of the columns. After this directive is specified, the following
directives should use the new names of the columns specified by this directive.

**Specification**
```
  set columns {column-name-1},{column-name-2}, ... {column-name-3}
```
* {column-name-x} Specifies a list of column names to be assigned to column.

**Example**
```
  set columns id,fname,lname,email,address,city,state,zip
```

### Filter Row

Directive for filtering rows either based on a condition or based on regular expression. Upon execution of
this directive, the following directives would be excluded of the rows that were filtered by this directive.

Condition based filtering allows one to specify an expression that if results in 'true' would filter the row else
would pass the row as-is to the next directive.


**Specification**
```
  filter-row-by-condition {condition}
```

* condition - A JEXL expression.

**Example**
```
  set columns id,fname,lname,email,address,city,state,zip
  filter-row-by-condition id > 200
```

Regular expression based filtering applies an regular expression on the value of a column specified in the
directive.
```
  filter-row-by-regex {column-name} {regex}
```

**Specification**
* column-name - Name of the column on which regex is applied. The regex is actually applied on the value of the column.
* regex - Standard regular expression.

**Example**
```
  set columns id,fname,lname,email,address,city,state,zip
  filter-row-by-condition email .*@joltie.io
```

### Set Column with expression
Set column directive allows you assign the result of a expression specified in JEXL format to a column.
JEXL implements an Expression Language for expressing not so complex expressions. Syntax support JEXL are
available [here](http://commons.apache.org/proper/commons-jexl/reference/syntax.html).

**Specification**
```
  set column {column-name} {expression}
```

* column-name - Name of the column to which the result of expression is saved to.
* expression - Expression to be evaluated specified in Jexl syntax.

**Example**
```
  set column salary hrlywage * 160
  set column hrlywage Math:abs(toDouble(hrlywage))
```

### Mask Column
Data masking (also known as data scrambling and data anonymization) is the process of replacing sensitive
information with realistic, but scrubbed, data based on masking rules. This plugin supports two types of
 masking method

* Substitution based &
* Shuffle based

Substitution based masking allows you to mask data based on a masking pattern. The patterns are specified using
two main literals namely '#' (Pound) and 'x'. '#' specifies that input should be passed on to output, 'x' would replace
the input charater with it. Any other characters will be passed as it to the output.
This directive is mainly used for masking SSN, customer id, credit card numbers, etc.

**Specification**

```
  mask-number {column-name} {masking-pattern}
```
* column-name - Name of the column to which the masking pattern needs to be applied
* masking-pattern - Defines the pattern to be used for masking the column.

**Example**
```
  mask-number ssn xxx-xx-####
  mask-number credircard xxxx-xxxxxx-x####
```

Shuffle based masking allows one to replace the input with the same size random data. It replaces
numbers with random numbers and string of characters with random characters.

**Specification**
```
  mask-shuffle {column-name}
```

* column-name - Name of the column to be shuffle masked.

**Example**
```
  mask-shuffle address
```

### Date Transformation

Directive for transforming a date from one format to another, or for transforming from unix timetsamp to
a format of date.

To convert a date string from one format to another use the following directive.

**Specification**
```
  format-date {column-name} {source-date-format} {destination-date-format}
```
* column-name - Name of the column to convert from source to destination format.
* source-date-format - Specifies the format of date pattern.
* destination-date-format - Specifies the format of date pattern.

**Example**
```
  format-date date MM/dd/yyyy EEE, MMM d, ''yy
```

To convert from unix timestamp to a date format use the following directive
**Specificaton**

```
  format-unixtimestamp {column-name} {date-format}
```
* column-name - Name of the column that contains unix timestamp that needs to be converted to date-format
* date-format - Format to convert from unix timestamp.

**Example**
```
  format-unixtimestamp timestamp MM/dd/yyyy
```

## How to add a new Directive

Directives are executed as a step, so it's a simple two step process to actually implement the Step and
provide the specification for directive.

### Step 1/2
In order to add a new step for Wrangler plugin, implement the interface 'Step'.
```
/**
 * A interface defining the wrangle step in the wrangling pipeline.
 */
public interface Step {
  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return Wrangled {@link Row}.
   * @throws StepException In case of any issue this exception is thrown.
   */
  Row execute(Row row) throws StepException, SkipRowException;
}
```

### Step 2/2
Modify the specification to parse the directive specification and create the implementation of
Step you have created above.

### Directive Specification

Currently directives are specified as simple text. Below is sample of directives specified for transforming
the feed.

```
  01. set format csv , true
  02. set columns fname,lname,emailid,address,city,state,country,zip,hourlyrate,ssn,lastupdt
  03. rename fname first_name
  04. rename lname last_name
  05. drop city
  06. drop country
  07. merge first_name last_name full_name ,
  08. upper state
  09. lower email_id
  10. filter-row-by-regex emailid .*@gmail.com
  11. set column name concat(lname, \", \", fname)
  12. drop lname
  13. drop fname
  14. filter-row-by-condition hourlyrate > 12
  15. set column salary hourlyrate * 40 * 4
  16. mask-number ssn xxx-xx-####
  17. date-format lastupdt dd-MM-YYYY MM/dd/YYYY
```

## Build
To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

## UI Integration

The Cask Hydrator UI displays each plugin property as a simple textbox. To customize how the plugin properties
are displayed in the UI, you can place a configuration file in the ``widgets`` directory.
The file must be named following a convention of ``[plugin-name]-[plugin-type].json``.

See [Plugin Widget Configuration](http://docs.cdap.io/cdap/current/en/hydrator-manual/developing-plugins/packaging-plugins.html#plugin-widget-json)
for details on the configuration file.

The UI will also display a reference doc for your plugin if you place a file in the ``docs`` directory
that follows the convention of ``[plugin-name]-[plugin-type].md``.

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json file under the ``target`` directory. This file is deployed along with your .jar file to add your
plugins to CDAP.

## Deployment
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'my-plugins-1.0.0':

    > load artifact target/my-plugins-1.0.0.jar config-file target/my-plugins-1.0.0.json

## Mailing Lists

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


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

.. |(Hydrator)| image:: http://cask.co/wp-content/uploads/hydrator_logo_cdap1.png
