/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.parser;

import com.google.common.base.Joiner;
import edu.emory.mathcs.backport.java.util.Arrays;
import io.cdap.wrangler.api.GrammarMigrator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link GrammarMigrator}.
 */
public class GrammarMigratorTest {
  private static final String[] input = new String[] {
    "set column salary salary > 100 ? 100 : salary",
    "set columns fname,lname,address,city,state,country,zipcode",
    "rename body_fname fname",
    "set-type value int",
    "drop fname,lname,address,zipcode,city",
    "merge fname lname name ,",
    "uppercase name",
    "lowercase name",
    "titlecase name",
    "indexsplit department 1 10 zone",
    "split name , fname lname",
    "filter-row-if-matched address .*?",
    "filter-row-if-not-matched address .*?",
    "filter-row-if-true age < 10.0",
    "filter-row-if-false age < 10.0 && gender == 'm'",
    "set-variable test count > 10",
    "increment-variable test 1 count > 10",
    "mask-number ssn xxx-xx-####",
    "mask-shuffle address",
    "format-date startdate yyyy-MM-dd",
    "format-unix-timestamp timestamp yyyy/MM/dd",
    "quantize col1 col2 1:2=test,3:4=test1",
    "find-and-replace name s/test//g",
    "parse-as-csv body , true",
    "parse-as-json body 1",
    "parse-as-protobuf body test event 1.0",
    "json-path source target a/b/c",
    "set-charset body utf-8",
    "invoke-http http://a.b/json fname,lname,address a=b,x=y",
    "set-record-delim body , 10",
    "parse-as-fixed-length body 3,4,5,6,7,8 #",
    "split-to-rows body ,",
    "split-to-columns body ,",
    "parse-xml-to-json body 1",
    "parse-as-xml xml",
    "xpath name fname /items/item/first_name",
    "xpath-array name fname /items/item/first_name",
    "flatten a,b,c,d",
    "copy source target true",
    "fill-null-or-empty value ,",
    "cut-character phone areacode 1-3",
    "generate-uuid ssn",
    "url-encode url",
    "url-decode url",
    "parse-as-log body %m-%y-%{HOSTNAME}",
    "parse-as-date date UTC",
    "parse-as-simple-date date yyyy-MM-dd",
    "diff-date date1 date2 diffdate",
    "keep fname,lname,address,city,zipcode",
    "parse-as-hl7 body 1",
    "split-email email",
    "swap col1 col2",
    "hash col SHA1 true",
    "write-as-json-map output",
    "write-as-json-object output fname,lname,address",
    "write-as-csv output",
    "parse-as-avro-file body",
    "send-to-error Fare < 8.06",
    "fail Fare < 8.06",
    "text-distance abc col1 col2 output",
    "text-metric abc col1 col2 output",
    "catalog-lookup ICD-9 value",
    "table-lookup value mylookup-table",
    "stemming text",
    "columns-replace s/body_//g",
    "extract-regex-groups body s/body_//g",
    "split-url url",
    "cleanse-column-names",
    "change-column-case upper",
    "set-column value output > 10 ? 'test' : 'non-test'",
    "encode base64 binary",
    "decode base64 binary",
    "trim name",
    "ltrim name",
    "rtrim name"
  };

  private static final String[] output = new String[] {
    "set-column :salary exp:{salary > 100 ? 100 : salary};",
    "set-headers :fname,:lname,:address,:city,:state,:country,:zipcode;",
    "rename :body_fname :fname;",
    "set-type :value int null null null;",
    "drop :fname,:lname,:address,:zipcode,:city;",
    "merge :fname :lname :name ',';",
    "uppercase :name;",
    "lowercase :name;",
    "titlecase :name;",
    "indexsplit :department 1 10 :zone;",
    "split :name ',' :fname :lname;",
    "filter-by-regex if-matched :address '.*?';",
    "filter-by-regex if-not-matched :address '.*?';",
    "filter-row exp:{age < 10.0} true;",
    "filter-row exp:{age < 10.0 && gender == 'm'} false;",
    "set-variable test exp:{count > 10};",
    "increment-variable test 1 exp:{count > 10};",
    "mask-number :ssn 'xxx-xx-####';",
    "mask-shuffle :address;",
    "format-date :startdate 'yyyy-MM-dd';",
    "format-unix-timestamp :timestamp 'yyyy/MM/dd';",
    "quantize :col1 :col2 1:2=test,3:4=test1;",
    "find-and-replace :name 's/test//g';",
    "parse-as-csv :body ',' true;",
    "parse-as-json :body 1;",
    "parse-as-protobuf :body test 'event' 1.0;",
    "json-path :source :target 'a/b/c';",
    "set-charset :body utf-8;",
    "invoke-http 'http://a.b/json' :fname,:lname,:address 'a=b,x=y';",
    "set-record-delim :body ',' 10;",
    "parse-as-fixed-length :body 3,4,5,6,7,8 '#';",
    "split-to-rows :body ',';",
    "split-to-columns :body ',';",
    "parse-xml-to-json :body 1;",
    "parse-as-xml :xml;",
    "xpath :name :fname '/items/item/first_name';",
    "xpath-array :name :fname '/items/item/first_name';",
    "flatten :a,:b,:c,:d;",
    "copy :source :target true;",
    "fill-null-or-empty :value ',';",
    "cut-character :phone :areacode '1-3';",
    "generate-uuid :ssn;",
    "url-encode :url;",
    "url-decode :url;",
    "parse-as-log :body '%m-%y-%{HOSTNAME}';",
    "parse-as-date :date 'UTC';",
    "parse-as-simple-date :date 'yyyy-MM-dd';",
    "diff-date :date1 :date2 :diffdate;",
    "keep :fname,:lname,:address,:city,:zipcode;",
    "parse-as-hl7 :body 1;",
    "split-email :email;",
    "swap :col1 :col2;",
    "hash :col 'SHA1' true;",
    "write-as-json-map :output;",
    "write-as-json-object :output :fname,:lname,:address;",
    "write-as-csv :output;",
    "parse-as-avro-file :body;",
    "send-to-error exp:{Fare < 8.06};",
    "fail exp:{Fare < 8.06};",
    "text-distance 'abc' :col1 :col2 :output;",
    "text-metric 'abc' :col1 :col2 :output;",
    "catalog-lookup 'ICD-9' :value;",
    "table-lookup :value 'mylookup-table';",
    "stemming :text;",
    "columns-replace 's/body_//g';",
    "extract-regex-groups :body 's/body_//g';",
    "split-url :url;",
    "cleanse-column-names;",
    "change-column-case upper;",
    "set-column :value exp:{output > 10 ? 'test' : 'non-test'};",
    "encode 'base64' :binary;",
    "decode 'base64' :binary;",
    "trim :name;",
    "ltrim :name;",
    "rtrim :name;"
  };

  @Test
  public void testMigration() throws Exception {
    List<String> expected = Arrays.asList(output);
    GrammarMigrator migrator = new MigrateToV2(input);
    String actual = migrator.migrate();
    Assert.assertEquals(Joiner.on('\n').join(expected), actual);
  }
}
