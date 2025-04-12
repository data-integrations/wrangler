Data Prep









A collection of libraries, a pipeline plugin, and a CDAP service for performing data cleansing, transformation, and filtering using a set of data manipulation instructions (directives). These instructions are either generated using an interactive visual tool or are manually created.

Data Prep defines a few concepts that might be useful if you are just getting started with it. Learn about them here

The Data Prep Transform is separately documented.

Data Prep Cheatsheet

New Features
More here on upcoming features.

User Defined Directives, also known as UDD, allow you to create custom functions to transform records within CDAP DataPrep or a.k.a Wrangler. CDAP comes with a comprehensive library of functions. There are however some omissions, and some specific cases for which UDDs are the solution. Additional information on how you can build your custom directives here.

Migrating directives from version 1.0 to version 2.0 here

Information about Grammar here

Various TokenType supported by the system here

Custom Directive Implementation Internals here

A new capability that allows CDAP Administrators to restrict the directives that are accessible to their users. More information on configuring can be found here

Demo Videos and Recipes
Videos and Screencasts are the best way to learn, so we have compiled simple, short screencasts that show some of the features of Data Prep. Additional videos can be found here

Videos
[SCREENCAST] Creating Lookup Dataset and Joining

[SCREENCAST] Restricted Directives

[SCREENCAST] Parse Excel files in CDAP

[SCREENCAST] Parse File As AVRO File

[SCREENCAST] Parsing Binary Coded AVRO Messages

[SCREENCAST] Parsing Binary Coded AVRO Messages & Protobuf messages using schema registry

[SCREENCAST] Quantize a column - Digitize

[SCREENCAST] Data Cleansing capability with send-to-error directive

[SCREENCAST] Building Data Prep from the GitHub source

[VOICE-OVER] End-to-End Demo Video

[SCREENCAST] Ingesting into Kudu

[SCREENCAST] Realtime HL7 CCDA XML from Kafka into Time Partitioned Parquet

[SCREENCAST] Parsing JSON file

[SCREENCAST] Flattening arrays

[SCREENCAST] Data cleansing with send-to-error directive

[SCREENCAST] Publishing to Kafka

[SCREENCAST] Fixed length to JSON

Recipes
Parsing Apache Log Files

Parsing CSV Files and Extracting Column Values

Parsing HL7 CCDA XML Files

Available Directives
These directives are currently available:

Directive	Description
Parsers	
JSON Path	Uses a DSL (a JSON path expression) for parsing JSON records
Parse as AVRO	Parsing an AVRO encoded message - either as binary or json
Parse as AVRO File	Parsing an AVRO data file
Parse as CSV	Parsing an input record as comma-separated values
Parse as Date	Parsing dates using natural language processing
Parse as Excel	Parsing excel file.
Parse as Fixed Length	Parses as a fixed length record with specified widths
Parse as HL7	Parsing Health Level 7 Version 2 (HL7 V2) messages
Parse as JSON	Parsing a JSON object
Parse as Log	Parses access log files as from Apache HTTPD and nginx servers
Parse as Protobuf	Parses a Protobuf encoded in-memory message using descriptor
Parse as Simple Date	Parses date strings
Parse XML To JSON	Parses an XML document into a JSON structure
Parse as Currency	Parses a string representation of currency into a number.
Parse as Datetime	Parses strings with datetime values to CDAP datetime type
Performance
Initial performance tests show that with a set of directives of high complexity for transforming data, DataPrep is able to process at about ~106K records per second. The rates below are specified as records/second.

Directive Complexity	Column Count	Records	Size	Mean Rate
High (167 Directives)	426	127,946,398	82,677,845,324	106,367.27
High (167 Directives)	426	511,785,592	330,711,381,296	105,768.93
Contact
Mailing Lists
CDAP User Group and Development Discussions:

cdap-user@googlegroups.com

The cdap-user group is for users of CDAP to discuss and ask questions. You can also send questions to the CDAP Slack channels #general or #wrangler.
