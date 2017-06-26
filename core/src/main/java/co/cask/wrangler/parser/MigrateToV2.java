/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.parser;

import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.GrammarMigrator;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static co.cask.wrangler.parser.SimpleTextParser.getNextToken;

/**
 * This class helps rewrites the recipe of directives from version 1.0 to version 2.0.
 */
public final class MigrateToV2 implements GrammarMigrator {

  /**
   * Checks to see if directive is migratable.
   *
   * @param directives to be checked if it's migratable.
   * @return true if the directives are migrateable, false otherwise.
   */
  @Override
  public boolean isMigrateable(List<String> directives) {
    boolean migrateable = true;

    for (String directive : directives) {
      directive = directive.trim();
      if (directive.isEmpty() || directive.startsWith("//")) {
        continue;
      }

      StringTokenizer tokenizer = new StringTokenizer(directive, " ");
      String command = tokenizer.nextToken();

      if (command.equalsIgnoreCase("#pragma")) {
        migrateable = false;
        break;
      }
    }

    return migrateable;
  }

  /**
   * Rewrites the directives in version 1.0 to version 2.0.
   *
   * @param directives that need to be converted to version 2.0.
   * @return directives converted to version 2.0.
   */
  @Override
  public List<String> migrate(List<String> directives) throws DirectiveParseException {
    List<String> transformed = new ArrayList<>();
    int lineno = 1;
    for (String directive : directives) {
      directive = directive.trim();
      if (directive.isEmpty() || directive.startsWith("//")
        || (directive.startsWith("#") && !directive.startsWith("#pragma"))) {
        continue;
      }

      StringTokenizer tokenizer = new StringTokenizer(directive, " ");
      String command = tokenizer.nextToken();

      switch (command) {
        case "set": {
          switch (tokenizer.nextToken()) {
            // set column <column-name> <jexl-expression>
            case "column": {
              String column = getNextToken(tokenizer, "set column", "column-name", lineno);
              String expr = getNextToken(tokenizer, "\n", "set column", "jexl-expression", lineno);
              transformed.add(String.format("set-column :%s exp:{%s};", column, expr));
            }
            break;

            // set columns <name1, name2, ...>
            case "columns": {
              String columns = getNextToken(tokenizer, "\n", "set columns", "name1, name2, ...", lineno);
              String cols[] = columns.split(",");
              transformed.add(String.format("set-columns %s;", toColumArray(cols)));
            }
            break;
          }
        }
        break;

        // rename <old> <new>
        case "rename": {
          String oldcol = getNextToken(tokenizer,  command, "old", lineno);
          String newcol = getNextToken(tokenizer, command, "new", lineno);
          transformed.add(String.format("rename :%s :%s;", oldcol, newcol));
        }
        break;

        //set-type <column> <type>
        case "set-type": {
          String col = getNextToken(tokenizer,  command, "col", lineno);
          String type = getNextToken(tokenizer, command, "type", lineno);
          transformed.add(String.format("set-type :%s %s;", col, type));
        }
        break;

        // drop <column>[,<column>]
        case "drop": {
          String columns = getNextToken(tokenizer, command, "column", lineno);
          String cols[] = columns.split(",");
          transformed.add(String.format("drop %s;", toColumArray(cols)));
        }
        break;

        // merge <first> <second> <new-column> <seperator>
        case "merge": {
          String col1 = getNextToken(tokenizer, command, "first", lineno);
          String col2 = getNextToken(tokenizer, command, "second", lineno);
          String dest = getNextToken(tokenizer, command, "new-column", lineno);
          String delimiter = getNextToken(tokenizer, "\n", command, "delimiter", lineno);
          transformed.add(String.format("merge :%s :%s :%s '%s';", col1, col2, dest, delimiter));
        }
        break;

        // uppercase <col>
        case "uppercase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          transformed.add(String.format("uppercase :%s;", col));
        }
        break;

        // lowercase <col>
        case "lowercase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          transformed.add(String.format("lowercase :%s;", col));
        }
        break;

        // titlecase <col>
        case "titlecase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          transformed.add(String.format("titlecase :%s;", col));
        }
        break;

        // indexsplit <source> <start> <end> <destination>
        case "indexsplit": {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String startStr = getNextToken(tokenizer, command, "start", lineno);
          String endStr = getNextToken(tokenizer, command, "end", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          transformed.add(String.format("indexsplit :%s %s %s :%s;", source, startStr, endStr, destination));
        }
        break;

        // split <source-column-name> <delimiter> <new-column-1> <new-column-2>
        case "split": {
          String source = getNextToken(tokenizer, command, "source-column-name", lineno);
          String delimiter = getNextToken(tokenizer, command, "delimiter", lineno);
          String firstCol = getNextToken(tokenizer, command, "new-column-1", lineno);
          String secondCol = getNextToken(tokenizer, command, "new-column-2", lineno);
          transformed.add(String.format("split :%s '%s' :%s :%s;", source, delimiter, firstCol, secondCol));
        }
        break;

        // filter-row-if-matched <column> <regex>
        case "filter-row-if-matched": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
          transformed.add(String.format("filter-row-if-matched :%s '%s';", column, pattern));
        }
        break;

        // filter-row-if-not-matched <column> <regex>
        case "filter-row-if-not-matched": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
          transformed.add(String.format("filter-row-if-not-matched :%s '%s';", column, pattern));
        }
        break;

        // filter-row-if-true  <condition>
        case "filter-row-if-true": {
          String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
          transformed.add(String.format("filter-row-if-true exp:{%s};", condition));
        }
        break;

        // filter-row-if-false  <condition>
        case "filter-row-if-false": {
          String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
          transformed.add(String.format("filter-row-if-false exp:{%s};", condition));
        }
        break;

        // set-variable <variable> <expression>
        case "set-variable": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String expression = getNextToken(tokenizer, "\n", command, "expression", lineno);
          transformed.add(String.format("set-variable %s exp:{%s};", column, expression));
        }
        break;

        // increment-variable <variable> <value> <expression>
        case "increment-variable": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String value = getNextToken(tokenizer, command, "value", lineno);
          String expression = getNextToken(tokenizer, "\n", command, "expression", lineno);
          transformed.add(String.format("increment-variable %s %s exp:{%s};", column, value, expression));
        }
        break;

        // mask-number <column> <pattern>
        case "mask-number": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String mask = getNextToken(tokenizer, command, "pattern", lineno);
          transformed.add(String.format("mask-number :%s '%s';", column, mask));
        }
        break;

        // mask-shuffle <column>
        case "mask-shuffle": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("mask-shuffle :%s;", column));
        }
        break;

        // format-date <column> <destination>
        case "format-date": {
          String column = getNextToken(tokenizer, command, "column", 1);
          String format = getNextToken(tokenizer, "\n", command, "format", lineno);
          transformed.add(String.format("format-date :%s '%s';", column, format));
        }
        break;

        // format-unix-timestamp <column> <destination-format>
        case "format-unix-timestamp": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String dstDatePattern = getNextToken(tokenizer, "\n", command, "destination-format", lineno);
          transformed.add(String.format("format-unix-timestamp :%s '%s';", column, dstDatePattern));
        }
        break;

        // quantize <source-column> <destination-column> <[range1:range2)=value>,[<range1:range2=value>]*
        case "quantize": {
          String column1 = getNextToken(tokenizer, command, "source-column", lineno);
          String column2 = getNextToken(tokenizer, command, "destination-column", lineno);
          String ranges = getNextToken(tokenizer, "\n", command, "destination-column", lineno);
          transformed.add(String.format("quantize :%s :%s %s;", column1, column2, ranges));
        }
        break;

        // find-and-replace <column> <sed-script>
        case "find-and-replace" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String expression = getNextToken(tokenizer, "\n", command, "sed-script", lineno);
          transformed.add(String.format("find-and-replace :%s '%s';", column, expression));
        }
        break;

        // parse-as-csv <column> <delimiter> [<header=true/false>]
        case "parse-as-csv" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String delimStr = getNextToken(tokenizer, command, "delimiter", lineno);
          String hasHeaderLinesOpt = getNextToken(tokenizer, "\n", command, "true|false", lineno, true);
          transformed.add(String.format("parse-as-csv :%s '%s' %s;", column, delimStr, hasHeaderLinesOpt));
        }
        break;

        // parse-as-json <column> [depth]
        case "parse-as-json" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          transformed.add(String.format("parse-as-json :%s %s;", column, depthOpt));
        }
        break;

        // parse-as-avro <column> <schema-id> <json|binary> [version]
        case "parse-as-avro" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String schemaId = getNextToken(tokenizer, command, "schema-id", lineno);
          String type = getNextToken(tokenizer, command, "type", lineno);
          String versionOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          transformed.add(String.format("parse-as-avro :%s %s %s %s;", column, schemaId, type, versionOpt));
        }
        break;

        // parse-as-protobuf <column> <schema-id> <record-name> [version]
        case "parse-as-protobuf" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String schemaId = getNextToken(tokenizer, command, "schema-id", lineno);
          String recordName = getNextToken(tokenizer, command, "record-name", lineno);
          String versionOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          transformed.add(String.format("parse-as-protobuf :%s %s '%s' '%s';", column, schemaId, recordName, versionOpt));
        }
        break;

        // json-path <source> <destination> <json-path>
        case "json-path" : {
          String src = getNextToken(tokenizer, command, "source", lineno);
          String dest = getNextToken(tokenizer, command, "dest", lineno);
          String path = getNextToken(tokenizer, "\n", command, "json-path", lineno);
          transformed.add(String.format("json-path :%s :%s '%s';", src, dest, path));
        }
        break;

        // set-charset <column> <charset>
        case "set-charset" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String charset = getNextToken(tokenizer, "\n", command, "charset", lineno, true);
          transformed.add(String.format("set-charset :%s %s;", column, charset));
        }
        break;

        // invoke-http <url> <column>[,<column>] <header>[,<header>]
        case "invoke-http" : {
          String url = getNextToken(tokenizer, command, "url", lineno);
          String columnsOpt = getNextToken(tokenizer, command, "columns", lineno);
          String headers = getNextToken(tokenizer, "\n", command, "headers", lineno, true);
          transformed.add(String.format("invoke-http '%s' %s '%s';", url,
                                        toColumArray(columnsOpt.split(",")), headers));
        }
        break;

        // set-record-delim <column> <delimiter> [<limit>]
        case "set-record-delim" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String delimiter = getNextToken(tokenizer, command, "delimiter", lineno);
          String limitStr = getNextToken(tokenizer, "\n", column, "limit", lineno, true);
          transformed.add(String.format("set-record-delim :%s '%s' %s;", column, delimiter, limitStr));
        }
        break;

        // parse-as-fixed-length <column> <widths> [<padding>]
        case "parse-as-fixed-length" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String widthStr = getNextToken(tokenizer, command, "widths", lineno);
          String padding = getNextToken(tokenizer, "\n", column, "padding", lineno, true);
          transformed.add(String.format("parse-as-fixed-length :%s %s '%s';", column, widthStr, padding));
        }
        break;

        // split-to-rows <column> <separator>
        case "split-to-rows" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String regex = getNextToken(tokenizer, "\n", "separator", lineno);
          transformed.add(String.format("split-to-rows :%s '%s';", column, regex));
        }
        break;

        // split-to-columns <column> <regex>
        case "split-to-columns" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String regex = getNextToken(tokenizer, "\n", "regex", lineno);
          transformed.add(String.format("split-to-columns :%s '%s';", column, regex));
        }
        break;

        // parse-xml-to-json <column> [<depth>]
        case "parse-xml-to-json" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          transformed.add(String.format("parse-xml-to-json :%s %s;", column, depthOpt));
        }
        break;

        // parse-as-xml <column>
        case "parse-as-xml" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("parse-as-xml :%s;", column));
        }
        break;

        // parse-as-excel <column> <sheet number | sheet name>
        case "parse-as-excel" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String sheet = getNextToken(tokenizer, "\n", command, "sheet", lineno, true);
          transformed.add(String.format("parse-as-excel :%s '%s';", column, sheet));
        }
        break;

        // xpath <column> <destination> <xpath>
        case "xpath" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String xpath = getNextToken(tokenizer, "\n", command, "xpath", lineno);
          transformed.add(String.format("xpath :%s :%s '%s';", column, destination, xpath));
        }
        break;

        // xpath-array <column> <destination> <xpath>
        case "xpath-array" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String xpath = getNextToken(tokenizer, "\n", command, "xpath", lineno);
          transformed.add(String.format("xpath-array :%s :%s '%s';", column, destination, xpath));
        }
        break;

        // flatten <column>[,<column>,<column>,...]
        case "flatten" : {
          String cols = getNextToken(tokenizer, command, "columns", lineno);
          transformed.add(String.format("flatten %s;", toColumArray(cols.split(","))));
        }
        break;

        // copy <source> <destination> [force]
        case "copy" : {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String forceOpt = getNextToken(tokenizer, "\n", command, "force", lineno, true);
          transformed.add(String.format("copy :%s :%s %s;", source, destination, forceOpt));
        }
        break;

        // fill-null-or-empty <column> <fixed value>
        case "fill-null-or-empty" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String value = getNextToken(tokenizer, command, "fixed-value", lineno);
          transformed.add(String.format("fill-null-or-empty :%s '%s';", column, value));
        }
        break;

        // cut-character <source> <destination> <range|indexes>
        case "cut-character" : {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String range = getNextToken(tokenizer, command, "range", lineno);
          transformed.add(String.format("cut-character :%s :%s '%s';", source, destination, range));
        }
        break;

        // generate-uuid <column>
        case "generate-uuid" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("generate-uuid :%s;", column));
        }
        break;

        // url-encode <column>
        case "url-encode" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("url-encode :%s;", column));
        }
        break;

        // url-decode <column>
        case "url-decode" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("url-decode :%s;", column));
        }
        break;

        // parse-as-log <column> <format>
        case "parse-as-log" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String format = getNextToken(tokenizer, "\n", command, "format", lineno);
          transformed.add(String.format("parse-as-log :%s '%s';", column, format));
        }
        break;

        // parse-as-date <column> [<timezone>]
        case "parse-as-date" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String timezone = getNextToken(tokenizer, "\n", command, "timezone", lineno, true);
          transformed.add(String.format("parse-as-date :%s '%s';", column, timezone));
        }
        break;

        // parse-as-simple-date <column> <pattern>
        case "parse-as-simple-date" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String pattern = getNextToken(tokenizer, "\n", command, "format", lineno);
          transformed.add(String.format("parse-as-simple-date :%s '%s';", column, pattern));
        }
        break;

        // diff-date <column1> <column2> <destColumn>
        case "diff-date" : {
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destColumn = getNextToken(tokenizer, "\n", command, "destColumn", lineno);
          transformed.add(String.format("diff-date :%s :%s :%s;", column1, column2, destColumn));
        }
        break;

        // keep <column>[,<column>]*
        case "keep" : {
          String columns = getNextToken(tokenizer, command, "columns", lineno);
          transformed.add(String.format("keep %s;", toColumArray(columns.split(","))));
        }
        break;

        // parse-as-hl7 <column> [<depth>]
        case "parse-as-hl7" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          transformed.add(String.format("parse-as-hl7 :%s %s;", column, depthOpt));
        }
        break;

        // split-email <column>
        case "split-email" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("split-email :%s;", column));
        }
        break;

        // swap <column1> <column2>
        case "swap" : {
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          transformed.add(String.format("swap :%s :%s;", column1, column2));
        }
        break;

        // hash <column> <algorithm> [encode]
        case "hash" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String algorithm = getNextToken(tokenizer, command, "algorithm", lineno);
          String encodeOpt = getNextToken(tokenizer, "\n", command, "encode", lineno, true);
          transformed.add(String.format("hash :%s '%s' %s;", column, algorithm, encodeOpt));
        }
        break;

        // write-as-json-map <column>
        case "write-as-json-map" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("write-as-json-map :%s;", column));
        }
        break;

        // write-as-json-object <dest-column> [<src-column>[,<src-column>]
        case "write-as-json-object" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String columnsStr = getNextToken(tokenizer, "\n", command, "columns", lineno);
          transformed.add(String.format("write-as-json-object :%s %s;", column, toColumArray(columnsStr.split(","))));
        }
        break;

        // write-as-csv <column>
        case "write-as-csv" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("write-as-csv :%s;", column));
        }
        break;

        // filter-rows-on condition-false <boolean-expression>
        // filter-rows-on condition-true <boolean-expression>
        // filter-rows-on empty-or-null-columns <column>[,<column>*]
        // filter-rows-on regex-match <regex>
        // filter-rows-on regex-not-match <regex>
        case "filter-rows-on" : {
          String cmd = getNextToken(tokenizer, command, "command", lineno);
          if (cmd.equalsIgnoreCase("condition-false")) {
            String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
            transformed.add(String.format("filter-rows-on condition-false exp:{%s};", condition));
          } else if (cmd.equalsIgnoreCase("condition-true")) {
            String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
            transformed.add(String.format("filter-rows-on condition-true exp:{%s};", condition));
          } else if (cmd.equalsIgnoreCase("empty-or-null-columns")) {
            String columns = getNextToken(tokenizer, "\n", command, "columns", lineno);
            transformed.add(String.format("filter-rows-on empty-or-null-columns %s;", toColumArray(columns.split(","))));
          } else if (cmd.equalsIgnoreCase("regex-match")) {
            String column = getNextToken(tokenizer, command, "column", lineno);
            String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
            transformed.add(String.format("filter-rows-on regex-match :%s '%s';", column, pattern));
          } else if (cmd.equalsIgnoreCase("regex-not-match")) {
            String column = getNextToken(tokenizer, command, "column", lineno);
            String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
            transformed.add(String.format("filter-rows-on regex-not-match :%s '%s';", column, pattern));
          } else {
            throw new DirectiveParseException(
              String.format("Unknown option '%s' specified for filter-rows-on directive at line no %s", cmd, lineno)
            );
          }
        }
        break;

        // parse-as-avro-file <column>
        case "parse-as-avro-file": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("parse-as-avro-file :%s;", column));
        }
        break;

        // send-to-error <condition>
        case "send-to-error": {
          String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
          transformed.add(String.format("send-to-error exp:{%s};", condition));
        }
        break;

        // fail <condition>
        case "fail": {
          String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
          transformed.add(String.format("fail exp:{%s};", condition));
        }
        break;

        // text-distance <method> <column1> <column2> <destination>
        case "text-distance" : {
          String method = getNextToken(tokenizer, command, "method", lineno);
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          transformed.add(String.format("text-distance %s :%s :%s :%s;", method, column1, column2, destination));
        }
        break;

        // text-metric <method> <column1> <column2> <destination>
        case "text-metric" : {
          String method = getNextToken(tokenizer, command, "method", lineno);
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          transformed.add(String.format("text-metric %s :%s :%s :%s;", method, column1, column2, destination));
        }
        break;

        // catalog-lookup ICD-9|ICD-10 <column>
        case "catalog-lookup" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("catalog-lookup '%s' :%s;", type, column));
        }
        break;

        // table-lookup <column> <table>
        case "table-lookup" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String table = getNextToken(tokenizer, command, "table", lineno);
          transformed.add(String.format("table-lookup :%s '%s';", column, table));
        }
        break;

        // stemming <column>
        case "stemming" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("stemming :%s;", column));
        }
        break;

        // columns <sed>
        case "columns-replace" : {
          String sed = getNextToken(tokenizer, command, "sed-expression", lineno);
          transformed.add(String.format("columns-replace '%s';", sed));
        }
        break;

        // extract-regex-groups <column> <regex>
        case "extract-regex-groups" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String regex = getNextToken(tokenizer, command, "regex", lineno);
          transformed.add(String.format("extract-regex-groups :%s '%s';", column, regex));
        }
        break;

        // split-url <column>
        case "split-url" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("split-url :%s;", column));
        }
        break;

        // cleanse-column-names
        case "cleanse-column-names" : {
          transformed.add("cleanse-column-names;");
        }
        break;

        // change-column-case <upper|lower|uppercase|lowercase>
        case "change-column-case" : {
          String casing = getNextToken(tokenizer, command, "case", lineno);
          transformed.add(String.format("change-column-case %s;", casing));
        }
        break;

        // set-column <column> <expression>
        case "set-column" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String expr = getNextToken(tokenizer, "\n", command, "expression", lineno);
          transformed.add(String.format("set-column :%s exp:{%s};", column, expr));
        }
        break;

        // encode <base32|base64|hex> <column>
        case "encode" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("encode %s :%s;", type, column));
        }
        break;

        // decode <base32|base64|hex> <column>
        case "decode" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          transformed.add(String.format("decode %s :%s;", type, column));
        }
        break;

        //trim <column>
        case "trim": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          transformed.add(String.format("trim :%s;", col));
        }
        break;

        //ltrim <column>
        case "ltrim": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          transformed.add(String.format("ltrim :%s;", col));
        }
        break;

        //rtrim <column>
        case "rtrim": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          transformed.add(String.format("rtrim :%s;", col));
        }
        break;

        default:
          if (!directive.endsWith(";")) {
            transformed.add(directive + ";");
          } else {
            transformed.add(directive);
          }
          break;
      }

      lineno = lineno + 1;
    }
    return transformed;
  }

  private static String toColumArray(String[] columns) {
    List<String> array = new ArrayList<>();
    for (String column : columns) {
      array.add(":" + column);
    }
    return Joiner.on(",").join(array);
  }
}
