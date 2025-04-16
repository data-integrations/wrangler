# Directive Migration

Following is the list of all the directives mapping from version 1.0 to version 2.0.
```Rewriter``` allows you to convert from old format to new. If the recipe doesn't include ```#pragma version 2.0```
the recipe is treated as version 1.0 and the rewriter is invoked, else new parser is used.

| Name                   | Directive - Version 1.0                                                  | Directive - Version 2.0                                                |
| -----------------------| ------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
|  SET                   |  set column salary salary > 100 ? 100 : salary                           |  set-column :salary exp:{salary > 100 ? 100 : salary};                   |
|  SET                   |  set columns fname,lname,address,city,state,country,zipcode              |  set-columns :fname,:lname,:address,:city,:state,:country,:zipcode;      |
|  RENAME                |  rename body_fname fname                                                 |  rename :body_fname :fname;                                              |
|  SET-TYPE              |  set-type value int                                                      |  set-type :value int;                                                    |
|  DROP                  |  drop fname,lname,address,zipcode,city                                   |  drop :fname,:lname,:address,:zipcode,:city;                             |
|  MERGE                 |  merge fname lname name ,                                                |  merge :fname :lname :name ',';                                          |
|  UPPERCASE             |  uppercase name                                                          |  uppercase :name;                                                        |
|  LOWERCASE             |  lowercase name                                                          |  lowercase :name;                                                        |
|  TITLECASE             |  titlecase name                                                          |  titlecase :name;                                                        |
|  INDEXSPLIT            |  indexsplit department 1 10 zone                                         |  indexsplit :department 1 10 :zone;                                      |
|  SPLIT                 |  split name , fname lname                                                |  split :name ',' :fname :lname;                                          |
|  FILTER-ROW-IF-MATCHED  |  filter-row-if-matched address .*?                                       |  filter-row-if-matched :address '.*?';                                   |
|  FILTER-ROW-IF-NOT-MATCHED  |  filter-row-if-not-matched address .*?                                   |  filter-row-if-not-matched :address '.*?';                               |
|  FILTER-ROW-IF-TRUE    |  filter-row-if-true age < 10.0                                           |  filter-row-if-true exp:{age < 10.0};                                    |
|  FILTER-ROW-IF-FALSE   |  filter-row-if-false age < 10.0 && gender == 'm'                         |  filter-row-if-false exp:{age < 10.0 && gender == 'm'};                  |
|  SET-VARIABLE          |  set-variable test count > 10                                            |  set-variable test exp:{count > 10};                                     |
|  INCREMENT-VARIABLE    |  increment-variable test 1 count > 10                                    |  increment-variable test 1 exp:{count > 10};                             |
|  MASK-NUMBER           |  mask-number ssn xxx-xx-####                                             |  mask-number :ssn 'xxx-xx-####';                                         |
|  MASK-SHUFFLE          |  mask-shuffle address                                                    |  mask-shuffle :address;                                                  |
|  FORMAT-DATE           |  format-date startdate yyyy-MM-dd                                        |  format-date :startdate 'yyyy-MM-dd';                                    |
|  FORMAT-UNIX-TIMESTAMP  |  format-unix-timestamp timestamp yyyy/MM/dd                              |  format-unix-timestamp :timestamp 'yyyy/MM/dd';                          |
|  QUANTIZE              |  quantize col1 col2 1:2=test,3:4=test1                                   |  quantize :col1 :col2 1:2=test,3:4=test1;                                |
|  FIND-AND-REPLACE      |  find-and-replace name s/test//g                                         |  find-and-replace :name 's/test//g';                                     |
|  PARSE-AS-CSV          |  parse-as-csv body , true                                                |  parse-as-csv :body ',' true;                                            |
|  PARSE-AS-JSON         |  parse-as-json body 1                                                    |  parse-as-json :body 1;                                                  |
|  PARSE-AS-PROTOBUF     |  parse-as-protobuf body test event 1.0                                   |  parse-as-protobuf :body test 'event' '1.0';                             |
|  JSON-PATH             |  json-path source target a/b/c                                           |  json-path :source :target 'a/b/c';                                      |
|  SET-CHARSET           |  set-charset body utf-8                                                  |  set-charset :body utf-8;                                                |
|  INVOKE-HTTP           |  invoke-http http://a.b/json fname,lname,address a=b,x=y                 |  invoke-http 'http://a.b/json' :fname,:lname,:address 'a=b,x=y';         |
|  SET-RECORD-DELIM      |  set-record-delim body , 10                                              |  set-record-delim :body ',' 10;                                          |
|  PARSE-AS-FIXED-LENGTH  |  parse-as-fixed-length body 3,4,5,6,7,8 #                                |  parse-as-fixed-length :body 3,4,5,6,7,8 '#';                            |
|  SPLIT-TO-ROWS         |  split-to-rows body ,                                                    |  split-to-rows :body ',';                                                |
|  SPLIT-TO-COLUMNS      |  split-to-columns body ,                                                 |  split-to-columns :body ',';                                             |
|  PARSE-XML-TO-JSON     |  parse-xml-to-json body 1                                                |  parse-xml-to-json :body 1;                                              |
|  PARSE-AS-XML          |  parse-as-xml xml                                                        |  parse-as-xml :xml;                                                      |
|  PARSE-AS-EXCEL        |  parse-as-excel body 0                                                   |  parse-as-excel :body '0';                                               |
|  XPATH                 |  xpath name fname /items/item/first_name                                 |  xpath :name :fname '/items/item/first_name';                            |
|  XPATH-ARRAY           |  xpath-array name fname /items/item/first_name                           |  xpath-array :name :fname '/items/item/first_name';                      |
|  FLATTEN               |  flatten a,b,c,d                                                         |  flatten :a,:b,:c,:d;                                                    |
|  COPY                  |  copy source target true                                                 |  copy :source :target true;                                              |
|  FILL-NULL-OR-EMPTY    |  fill-null-or-empty value ,                                              |  fill-null-or-empty :value ',';                                          |
|  CUT-CHARACTER         |  cut-character phone areacode 1-3                                        |  cut-character :phone :areacode '1-3';                                   |
|  GENERATE-UUID         |  generate-uuid ssn                                                       |  generate-uuid :ssn;                                                     |
|  URL-ENCODE            |  url-encode url                                                          |  url-encode :url;                                                        |
|  URL-DECODE            |  url-decode url                                                          |  url-decode :url;                                                        |
|  PARSE-AS-LOG          |  parse-as-log body %m-%y-%{HOSTNAME}                                     |  parse-as-log :body '%m-%y-%{HOSTNAME}';                                 |
|  PARSE-AS-DATE         |  parse-as-date date UTC                                                  |  parse-as-date :date 'UTC';                                              |
|  PARSE-AS-SIMPLE-DATE  |  parse-as-simple-date date yyyy-MM-dd                                    |  parse-as-simple-date :date 'yyyy-MM-dd';                                |
|  DIFF-DATE             |  diff-date date1 date2 diffdate                                          |  diff-date :date1 :date2 :diffdate;                                      |
|  KEEP                  |  keep fname,lname,address,city,zipcode                                   |  keep :fname,:lname,:address,:city,:zipcode;                             |
|  PARSE-AS-HL7          |  parse-as-hl7 body 1                                                     |  parse-as-hl7 :body 1;                                                   |
|  SPLIT-EMAIL           |  split-email email                                                       |  split-email :email;                                                     |
|  SWAP                  |  swap col1 col2                                                          |  swap :col1 :col2;                                                       |
|  HASH                  |  hash col SHA1 true                                                      |  hash :col 'SHA1' true;                                                  |
|  WRITE-AS-JSON-MAP     |  write-as-json-map output                                                |  write-as-json-map :output;                                              |
|  WRITE-AS-JSON-OBJECT  |  write-as-json-object output fname,lname,address                         |  write-as-json-object :output :fname,:lname,:address;                    |
|  WRITE-AS-CSV          |  write-as-csv output                                                     |  write-as-csv :output;                                                   |
|  FILTER-ROWS-ON        |  filter-rows-on condition-false output < 10                              |  filter-rows-on condition-false exp:{output < 10};                       |
|  FILTER-ROWS-ON        |  filter-rows-on condition-true output < 10                               |  filter-rows-on condition-true exp:{output < 10};                        |
|  FILTER-ROWS-ON        |  filter-rows-on empty-or-null-columns fname,lname                        |  filter-rows-on empty-or-null-columns :fname,:lname;                     |
|  FILTER-ROWS-ON        |  filter-rows-on regex-match col test*                                    |  filter-rows-on regex-match :col 'test*';                                |
|  FILTER-ROWS-ON        |  filter-rows-on regex-not-match col test*                                |  filter-rows-on regex-not-match :col 'test*';                            |
|  PARSE-AS-AVRO-FILE    |  parse-as-avro-file body                                                 |  parse-as-avro-file :body;                                               |
|  SEND-TO-ERROR         |  send-to-error Fare < 8.06                                               |  send-to-error exp:{Fare < 8.06};                                        |
|  FAIL                  |  fail Fare < 8.06                                                        |  fail exp:{Fare < 8.06};                                                 |
|  TEXT-DISTANCE         |  text-distance abc col1 col2 output                                      |  text-distance abc :col1 :col2 :output;                                  |
|  TEXT-METRIC           |  text-metric abc col1 col2 output                                        |  text-metric abc :col1 :col2 :output;                                    |
|  CATALOG-LOOKUP        |  catalog-lookup ICD-9 value                                              |  catalog-lookup 'ICD-9' :value;                                          |
|  TABLE-LOOKUP          |  table-lookup value mylookup-table                                       |  table-lookup :value 'mylookup-table';                                   |
|  STEMMING              |  stemming text                                                           |  stemming :text;                                                         |
|  COLUMNS-REPLACE       |  columns-replace s/body_//g                                              |  columns-replace 's/body_//g';                                           |
|  EXTRACT-REGEX-GROUPS  |  extract-regex-groups body s/body_//g                                    |  extract-regex-groups :body 's/body_//g';                                |
|  SPLIT-URL             |  split-url url                                                           |  split-url :url;                                                         |
|  CLEANSE-COLUMN-NAMES  |  cleanse-column-names                                                    |  cleanse-column-names;                                                   |
|  CHANGE-COLUMN-CASE    |  change-column-case upper                                                |  change-column-case upper;                                               |
|  SET-COLUMN            |  set-column value output > 10 ? 'test' : 'non-test'                      |  set-column :value exp:{output > 10 ? 'test' : 'non-test'};              |
|  ENCODE                |  encode base64 binary                                                    |  encode base64 :binary;                                                  |
|  DECODE                |  decode base64 binary                                                    |  decode base64 :binary;                                                  |
|  TRIM                  |  trim name                                                               |  trim :name;                                                             |
|  LTRIM                 |  ltrim name                                                              |  ltrim :name;                                                            |
|  RTRIM                 |  rtrim name                                                              |  rtrim :name;                                                            |
