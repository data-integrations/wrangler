/*
 * Copyright © 2016, 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.executor;

import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.steps.language.SetCharset;
import co.cask.wrangler.steps.column.ChangeColCaseNames;
import co.cask.wrangler.steps.column.CleanseColumnNames;
import co.cask.wrangler.steps.column.Columns;
import co.cask.wrangler.steps.column.ColumnsReplace;
import co.cask.wrangler.steps.column.Copy;
import co.cask.wrangler.steps.column.Drop;
import co.cask.wrangler.steps.column.Keep;
import co.cask.wrangler.steps.column.Merge;
import co.cask.wrangler.steps.column.Rename;
import co.cask.wrangler.steps.column.SplitToColumns;
import co.cask.wrangler.steps.column.Swap;
import co.cask.wrangler.steps.date.DiffDate;
import co.cask.wrangler.steps.date.FormatDate;
import co.cask.wrangler.steps.nlp.Stemming;
import co.cask.wrangler.steps.parser.CsvParser;
import co.cask.wrangler.steps.parser.FixedLengthParser;
import co.cask.wrangler.steps.parser.HL7Parser;
import co.cask.wrangler.steps.parser.JsPath;
import co.cask.wrangler.steps.parser.JsParser;
import co.cask.wrangler.steps.parser.ParseDate;
import co.cask.wrangler.steps.parser.ParseLog;
import co.cask.wrangler.steps.parser.ParseSimpleDate;
import co.cask.wrangler.steps.parser.XmlParser;
import co.cask.wrangler.steps.parser.XmlToJson;
import co.cask.wrangler.steps.row.Flatten;
import co.cask.wrangler.steps.row.RecordConditionFilter;
import co.cask.wrangler.steps.row.RecordMissingOrNullFilter;
import co.cask.wrangler.steps.row.RecordRegexFilter;
import co.cask.wrangler.steps.row.SendToError;
import co.cask.wrangler.steps.row.SetRecordDelimiter;
import co.cask.wrangler.steps.row.SplitToRows;
import co.cask.wrangler.steps.transformation.CatalogLookup;
import co.cask.wrangler.steps.transformation.CharacterCut;
import co.cask.wrangler.steps.transformation.Decode;
import co.cask.wrangler.steps.transformation.Encode;
import co.cask.wrangler.steps.transformation.Expression;
import co.cask.wrangler.steps.transformation.ExtractRegexGroups;
import co.cask.wrangler.steps.transformation.FillNullOrEmpty;
import co.cask.wrangler.steps.transformation.FindAndReplace;
import co.cask.wrangler.steps.transformation.GenerateUUID;
import co.cask.wrangler.steps.transformation.IndexSplit;
import co.cask.wrangler.steps.transformation.Lower;
import co.cask.wrangler.steps.transformation.MaskNumber;
import co.cask.wrangler.steps.transformation.MaskShuffle;
import co.cask.wrangler.steps.transformation.MessageHash;
import co.cask.wrangler.steps.transformation.Quantization;
import co.cask.wrangler.steps.transformation.SetColumn;
import co.cask.wrangler.steps.transformation.Split;
import co.cask.wrangler.steps.transformation.SplitEmail;
import co.cask.wrangler.steps.transformation.SplitURL;
import co.cask.wrangler.steps.transformation.TableLookup;
import co.cask.wrangler.steps.transformation.TextDistanceMeasure;
import co.cask.wrangler.steps.transformation.TextMetricMeasure;
import co.cask.wrangler.steps.transformation.TitleCase;
import co.cask.wrangler.steps.transformation.Upper;
import co.cask.wrangler.steps.transformation.UrlEncode;
import co.cask.wrangler.steps.transformation.XPathArrayElement;
import co.cask.wrangler.steps.transformation.XPathElement;
import co.cask.wrangler.steps.writer.WriteAsCSV;
import co.cask.wrangler.steps.writer.WriteAsJsonMap;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Parses the DSL into specification containing stepRegistry for wrangling.
 *
 * Following are some of the commands and format that {@link TextDirectives}
 * will handle.
 */
public class TextDirectives implements Directives {
  private static final Logger LOG = LoggerFactory.getLogger(TextDirectives.class);

  // directives for wrangling.
  private String[] directives;

  // Usage Registry
  private final UsageRegistry usageRegistry = new UsageRegistry();

  public TextDirectives(String[] directives) {
    this.directives = directives;
  }

  public TextDirectives(String directives) {
    this(directives.split("\n"));
  }

  public TextDirectives(List<String> directives) {
    this(directives.toArray(new String[directives.size()]));
  }

  /**
   * Parses the DSL to generate a sequence of stepRegistry to be executed by {@link co.cask.wrangler.api.Pipeline}.
   *
   * The transformation parsing here needs a better solution. It has many limitations and having different way would
   * allow us to provide much more advanced semantics for directives.
   *
   * @return List of stepRegistry to be executed.
   * @throws ParseException
   */
  private List<Step> parse() throws DirectiveParseException {
    List<Step> steps = new ArrayList<>();

    // Split directive by EOL
    int lineno = 1;

    // Iterate through each directive and create necessary stepRegistry.
    for (String directive : directives) {
      directive = directive.trim();
      if (directive.isEmpty() || directive.startsWith("//") || directive.startsWith("#")) {
        continue;
      }
      StringTokenizer tokenizer = new StringTokenizer(directive, " ");
      String command = tokenizer.nextToken();

      switch (command) {
        case "set": {
          switch (tokenizer.nextToken()) {
            // set format [csv|json] <delimiter> <skip empty lines>
            case "format": {
              String format = getNextToken(tokenizer, "set format", "[csv|json]", lineno);
              if (format.equalsIgnoreCase("csv")) {
                String delimStr = getNextToken(tokenizer, "set format", "delimiter", lineno);
                char delimiter = delimStr.charAt(0);
                if (delimStr.startsWith("\\")) {
                  String unescapedStr = StringEscapeUtils.unescapeJava(delimStr);
                  if (unescapedStr == null) {
                    throw new IllegalArgumentException("Invalid delimiter for CSV Parser: " + delimStr);
                  }
                  delimiter = unescapedStr.charAt(0);
                }
                boolean ignoreEmptyLines =
                  getNextToken(tokenizer, "set format", "true|false", lineno).equalsIgnoreCase("true");
                CsvParser.Options opt = new CsvParser.Options(delimiter, ignoreEmptyLines);
                steps.add(new CsvParser(lineno, directive, opt, STARTING_COLUMN, false));
                steps.add(new Drop(lineno, directive, STARTING_COLUMN));
              } else {
                throw new DirectiveParseException(
                  String.format("Unknown format '%s' specified at line %d", format, lineno)
                );
              }
            }
            break;

            // set column <column-name> <jexl-expression>
            case "column": {
              String column = getNextToken(tokenizer, "set column", "column-name", lineno);
              String expr = getNextToken(tokenizer, "\n", "set column", "jexl-expression", lineno);
              steps.add(new Expression(lineno, directive, column, expr));
            }
            break;

            // set columns <name1, name2, ...>
            case "columns": {
              String columns = getNextToken(tokenizer, "\n", "set columns", "name1, name2, ...", lineno);
              String cols[] = columns.split(",");
              steps.add(new Columns(lineno, directive, Arrays.asList(cols)));
            }
            break;
          }
        }
        break;

        // rename <old> <new>
        case "rename": {
          String oldcol = getNextToken(tokenizer,  command, "old", lineno);
          String newcol = getNextToken(tokenizer, command, "new", lineno);
          steps.add(new Rename(lineno, directive, oldcol, newcol));
        }
        break;

        // drop <column>[,<column>]
        case "drop": {
          String colums = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new Drop(lineno, directive, Arrays.asList(colums.split(","))));
        }
        break;

        // merge <first> <second> <new-column> <seperator>
        case "merge": {
          String col1 = getNextToken(tokenizer, command, "first", lineno);
          String col2 = getNextToken(tokenizer, command, "second", lineno);
          String dest = getNextToken(tokenizer, command, "new-column", lineno);
          String delimiter = getNextToken(tokenizer, command, "seperator", lineno);
          steps.add(new Merge(lineno, directive, col1, col2, dest, delimiter));
        }
        break;

        // uppercase <col>
        case "uppercase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          steps.add(new Upper(lineno, directive, col));
        }
        break;

        // lowercase <col>
        case "lowercase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          steps.add(new Lower(lineno, directive, col));
        }
        break;

        // titlecase <col>
        case "titlecase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          steps.add(new TitleCase(lineno, directive, col));
        }
        break;

        // indexsplit <source> <start> <end> <destination>
        case "indexsplit": {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String startStr = getNextToken(tokenizer, command, "start", lineno);
          String endStr = getNextToken(tokenizer, command, "end", lineno);
          int start = Integer.parseInt(startStr);
          int end = Integer.parseInt(endStr);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          steps.add(new IndexSplit(lineno, directive, source, start, end, destination));
        }
        break;

        // split <source-column-name> <delimiter> <new-column-1> <new-column-2>
        case "split": {
          String source = getNextToken(tokenizer, command, "source-column-name", lineno);
          String delimiter = getNextToken(tokenizer, command, "delimiter", lineno);
          String firstCol = getNextToken(tokenizer, command, "new-column-1", lineno);
          String secondCol = getNextToken(tokenizer, command, "new-column-2", lineno);
          steps.add(new Split(lineno, directive, source, delimiter, firstCol, secondCol));
        }
        break;

        // filter-row-if-matched <column> <regex>
        case "filter-row-if-matched": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
          steps.add(new RecordRegexFilter(lineno, directive, column, pattern, true));
        }
        break;

        // filter-row-if-not-matched <column> <regex>
        case "filter-row-if-not-matched": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
          steps.add(new RecordRegexFilter(lineno, directive, column, pattern, false));
        }
        break;

        // filter-row-if-true  <condition>
        case "filter-row-if-true": {
          String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
          steps.add(new RecordConditionFilter(lineno, directive, condition, true));
        }
        break;

        // filter-row-if-false  <condition>
        case "filter-row-if-false": {
          String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
          steps.add(new RecordConditionFilter(lineno, directive, condition, false));
        }
        break;

        // mask-number <column> <pattern>
        case "mask-number": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String mask = getNextToken(tokenizer, command, "pattern", lineno);
          steps.add(new MaskNumber(lineno, directive, column, mask));
        }
        break;

        // mask-shuffle <column>
        case "mask-shuffle": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new MaskShuffle(lineno, directive, column));
        }
        break;

        // format-date <column> <destination>
        case "format-date": {
          String column = getNextToken(tokenizer, command, "column", 1);
          String format = getNextToken(tokenizer, "\n", command, "format", lineno);
          steps.add(new FormatDate(lineno, directive, column, format));
        }
        break;

        // format-unix-timestamp <column> <destination-format>
        case "format-unix-timestamp": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String dstDatePattern = getNextToken(tokenizer, "\n", command, "destination-format", lineno);
          steps.add(new FormatDate(lineno, directive, column, dstDatePattern));
        }
        break;

        // quantize <source-column> <destination-column> <[range1:range2)=value>,[<range1:range2=value>]*
        case "quantize": {
          String column1 = getNextToken(tokenizer, command, "source-column", lineno);
          String column2 = getNextToken(tokenizer, command, "destination-column", lineno);
          String ranges = getNextToken(tokenizer, "\n", command, "destination-column", lineno);
          steps.add(new Quantization(lineno, directive, column1, column2, ranges));
        }
        break;

        // find-and-replace <column> <sed-script>
        case "find-and-replace" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String expression = getNextToken(tokenizer, "\n", command, "sed-script", lineno);
          steps.add(new FindAndReplace(lineno, directive, column, expression));
        }
        break;

        // parse-as-csv <column> <delimiter> [<header=true/false>]
        case "parse-as-csv" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String delimStr = getNextToken(tokenizer, command, "delimiter", lineno);
          char delimiter = delimStr.charAt(0);
          if (delimStr.startsWith("\\")) {
            String unescapedStr = StringEscapeUtils.unescapeJava(delimStr);
            if (unescapedStr == null) {
              throw new DirectiveParseException("Invalid delimiter for CSV Parser: " + delimStr);
            }
            delimiter = unescapedStr.charAt(0);
          }

          boolean hasHeader;
          String hasHeaderLinesOpt = getNextToken(tokenizer, "\n", command, "true|false", lineno, true);
          if (hasHeaderLinesOpt == null || hasHeaderLinesOpt.equalsIgnoreCase("false")) {
            hasHeader = false;
          } else {
            hasHeader = true;
          }
          CsvParser.Options opt = new CsvParser.Options(delimiter, true);
          steps.add(new CsvParser(lineno, directive, opt, column, hasHeader));
        }
        break;

        // parse-as-json <column> [depth]
        case "parse-as-json" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int depth = Integer.MAX_VALUE;
          if (depthOpt != null && !depthOpt.isEmpty()) {
            try {
              depth = Integer.parseInt(depthOpt);
            } catch (NumberFormatException e) {
              throw new DirectiveParseException(
                String.format("Depth '%s' specified is not a valid number.", depthOpt)
              );
            }
          }
          steps.add(new JsParser(lineno, directive, column, depth));
        }
        break;

        // json-path <source> <destination> <json-path>
        case "json-path" : {
          String src = getNextToken(tokenizer, command, "source", lineno);
          String dest = getNextToken(tokenizer, command, "dest", lineno);
          String path = getNextToken(tokenizer, "\n", command, "json-path", lineno);
          steps.add(new JsPath(lineno, directive, src, dest, path));
        }
        break;

        //set-charset <column> <charset>
        case "set-charset" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String charset = getNextToken(tokenizer, "\n", command, "charset", lineno, false);
          steps.add(new SetCharset(lineno, directive, column, charset));
        }
        break;

        // set-record-delim <column> <delimiter> [<limit>]
        case "set-record-delim" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String delimiter = getNextToken(tokenizer, command, "delimiter", lineno);
          String limitStr = getNextToken(tokenizer, "\n", column, "limit", lineno, true);
          if (limitStr == null || limitStr.isEmpty()) {
            limitStr = "1";
          }
          try {
            int limit = Integer.parseInt(limitStr);
            steps.add(new SetRecordDelimiter(lineno, directive, column, delimiter, limit));
          } catch (NumberFormatException e) {
            throw new DirectiveParseException(
              String.format("Limit '%s' specified is not a number.", limitStr)
            );
          }

        }
        break;

        // parse-as-fixed-length <column> <widths> [<padding>]
        case "parse-as-fixed-length" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String widthStr = getNextToken(tokenizer, command, "widths", lineno);
          String padding = getNextToken(tokenizer, "\n", column, "padding", lineno, true);
          if (padding == null || padding.isEmpty()) {
            padding = null; // Add space as padding.
          } else {
            padding = StringUtils.substringBetween(padding, "'", "'");
          }
          String[] widthsStr = widthStr.split(",");
          int[] widths = new int[widthsStr.length];
          int i = 0;
          for (String w : widthsStr) {
            try {
              widths[i] = Integer.parseInt(StringUtils.deleteWhitespace(w));
            } catch (NumberFormatException e) {
              throw new DirectiveParseException(
                String.format("Width specified '%s' at location %d is not a number.", w, i)
              );
            }
            ++i;
          }
          steps.add(new FixedLengthParser(lineno, directive, column, widths, padding));
        }
        break;

        // split-to-rows <column> <separator>
        case "split-to-rows" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String regex = getNextToken(tokenizer, "\n", "separator", lineno);
          steps.add(new SplitToRows(lineno, directive, column, regex));
        }
        break;

        // split-to-columns <column> <regex>
        case "split-to-columns" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String regex = getNextToken(tokenizer, "\n", "regex", lineno);
          steps.add(new SplitToColumns(lineno, directive, column, regex));
        }
        break;

        // parse-xml-to-json <column> [<depth>]
        case "parse-xml-to-json" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int depth = Integer.MAX_VALUE;
          try {
            if(depthOpt != null && !depthOpt.isEmpty()) {
              depth = Integer.parseInt(depthOpt);
            }
          } catch (NumberFormatException e) {
            throw new DirectiveParseException(e.getMessage());
          }
          steps.add(new XmlToJson(lineno, directive, column, depth));
        }
        break;

        // parse-as-xml <column>
        case "parse-as-xml" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new XmlParser(lineno, directive, column));
        }
        break;

        // xpath <column> <destination> <xpath>
        case "xpath" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String xpath = getNextToken(tokenizer, "\n", command, "xpath", lineno);
          steps.add(new XPathElement(lineno, directive, column, destination, xpath));
        }
        break;

        // xpath-array <column> <destination> <xpath>
        case "xpath-array" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String xpath = getNextToken(tokenizer, "\n", command, "xpath", lineno);
          steps.add(new XPathArrayElement(lineno, directive, column, destination, xpath));
        }
        break;

        // flatten <column>[,<column>,<column>,...]
        case "flatten" : {
          String cols = getNextToken(tokenizer, command, "columns", lineno);
          if (cols.equalsIgnoreCase("*")) {
            throw new DirectiveParseException(
              "Flatten does not support wildcard ('*') flattening. Please specify column names"
            );
          }

          String[] columns = cols.split(",");
          for (String column : columns) {
            if (column.trim().equalsIgnoreCase("*")) {
              throw new DirectiveParseException(
                "Flatten does not support wildcard ('*') flattening. Please specify column names"
              );
            }
          }
          steps.add(new Flatten(lineno, directive, columns));
        }
        break;

        // copy <source> <destination> [force]
        case "copy" : {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String forceOpt = getNextToken(tokenizer, "\n", command, "force", lineno, true);

          boolean force = false;
          if (forceOpt != null && forceOpt.equalsIgnoreCase("true")) {
            force = true;
          }
          steps.add(new Copy(lineno, directive, source, destination, force));
        }
        break;

        // fill-null-or-empty <column> <fixed value>
        case "fill-null-or-empty" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String value = getNextToken(tokenizer, command, "fixed-value", lineno);
          if (value != null && value.isEmpty()) {
            throw new DirectiveParseException(
              "Fixed value cannot be a empty string"
            );
          }
          steps.add(new FillNullOrEmpty(lineno, directive, column, value));
        }
        break;

        // cut-character <source> <destination> <range|indexes>
        case "cut-character" : {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String range = getNextToken(tokenizer, command, "range", lineno);
          steps.add(new CharacterCut(lineno, directive, source, destination, range));
        }
        break;

        // generate-uuid <column>
        case "generate-uuid" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new GenerateUUID(lineno, directive, column));
        }
        break;

        // url-encode <column>
        case "url-encode" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new UrlEncode(lineno, directive, column));
        }
        break;

        // url-decode <column>
        case "url-decode" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new UrlEncode(lineno, directive, column));
        }
        break;

        // parse-as-log <column> <format>
        case "parse-as-log" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String format = getNextToken(tokenizer, "\n", command, "format", lineno);
          steps.add(new ParseLog(lineno, directive, column, format));
        }
        break;

        // parse-as-date <column> [<timezone>]
        case "parse-as-date" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String timezone = getNextToken(tokenizer, "\n", command, "timezone", lineno, true);
          steps.add(new ParseDate(lineno, directive, column, timezone));
        }
        break;

        // parse-as-simple-date <column> <pattern>
        case "parse-as-simple-date" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String pattern = getNextToken(tokenizer, "\n", command, "format", lineno);
          steps.add(new ParseSimpleDate(lineno, directive, column, pattern));
        }
        break;

        // diff-date <column1> <column2> <destColumn>
        case "diff-date" : {
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destColumn = getNextToken(tokenizer, "\n", command, "destColumn", lineno);
          steps.add(new DiffDate(lineno, directive, column1, column2, destColumn));
        }
        break;

        // keep <column>[,<column>]*
        case "keep" : {
          String columns = getNextToken(tokenizer, command, "columns", lineno);
          steps.add(new Keep(lineno, directive, columns.split(",")));
        }
        break;

        // parse-as-hl7 <column> [<depth>]
        case "parse-as-hl7" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int depth = Integer.MAX_VALUE;
          try {
            if (depthOpt != null && !depthOpt.isEmpty()) {
              depth = Integer.parseInt(depthOpt);
            }
          } catch (NumberFormatException e) {
            throw new DirectiveParseException(e.getMessage());
          }
          steps.add(new HL7Parser(lineno, directive, column, depth));
        }
        break;
        
        // split-email <column>
        case "split-email" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new SplitEmail(lineno, directive, column));
        }
        break;

        // swap <column1> <column2>
        case "swap" : {
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          steps.add(new Swap(lineno, directive, column1, column2));
        }
        break;

        // hash <column> <algorithm> [encode]
        case "hash" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String algorithm = getNextToken(tokenizer, command, "algorithm", lineno);
          String encodeOpt = getNextToken(tokenizer, "\n", command, "encode", lineno, true);
          if (!MessageHash.isValid(algorithm)) {
            throw new DirectiveParseException(
              String.format("Algorithm '%s' specified in directive '%s' at line %d is not supported", algorithm,
                            command, lineno)
            );
          }

          boolean encode = true;
          if (encodeOpt.equalsIgnoreCase("false")) {
            encode = false;
          }

          try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            steps.add(new MessageHash(lineno, directive, column, digest, encode));
          } catch (NoSuchAlgorithmException e) {
            throw new DirectiveParseException(
              String.format("Unable to find algorithm specified '%s' in directive '%s' at line %d.",
                            algorithm, command, lineno)
            );
          }
        }
        break;

        // write-as-json <column>
        case "write-as-json-map" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new WriteAsJsonMap(lineno, directive, column));
        }
        break;

        // write-as-csv <column>
        case "write-as-csv" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new WriteAsCSV(lineno, directive, column));
        }
        break;

        //filter-rows-on condition-true <boolean-expression>
        //filter-rows-on condition-false <boolean-expression>
        //filter-rows-on regex-match <regex>
        //filter-rows-on regex-not-match <regex>
        //filter-rows-on empty-or-null-columns <column>[,<column>]*
        case "filter-rows-on" : {
          String cmd = getNextToken(tokenizer, command, "command", lineno);
          if (cmd.equalsIgnoreCase("condition-true")) {
            String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
            steps.add(new RecordConditionFilter(lineno, directive, condition, true));
          } else if (cmd.equalsIgnoreCase("condition-false")) {
            String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
            steps.add(new RecordConditionFilter(lineno, directive, condition, false));
          } else if (cmd.equalsIgnoreCase("regex-match")) {
            String column = getNextToken(tokenizer, command, "column", lineno);
            String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
            steps.add(new RecordRegexFilter(lineno, directive, column, pattern, true));
          } else if (cmd.equalsIgnoreCase("regex-not-match")) {
            String column = getNextToken(tokenizer, command, "column", lineno);
            String pattern = getNextToken(tokenizer, "\n", command, "regex", lineno);
            steps.add(new RecordRegexFilter(lineno, directive, column, pattern, false));
          } else if (cmd.equalsIgnoreCase("empty-or-null-columns")) {
            String columns = getNextToken(tokenizer, "\n", command, "columns", lineno);
            steps.add(new RecordMissingOrNullFilter(lineno, directive, columns.split(",")));
          } else {
            throw new DirectiveParseException(
              String.format("Unknown option '%s' specified for filter-rows-on directive at line no %s", cmd, lineno)
            );
          }
        }
        break;

        // send-to-error <condition>
        case "send-to-error": {
          String condition = getNextToken(tokenizer, "\n", command, "condition", lineno);
          steps.add(new SendToError(lineno, directive, condition));
        }
        break;

        // text-distance <method> <column1> <column2> <destination>
        case "text-distance" : {
          String method = getNextToken(tokenizer, command, "method", lineno);
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          steps.add(new TextDistanceMeasure(lineno, directive, method, column1, column2, destination));
        }
        break;

        // text-metric <method> <column1> <column2> <destination>
        case "text-metric" : {
          String method = getNextToken(tokenizer, command, "method", lineno);
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          steps.add(new TextMetricMeasure(lineno, directive, method, column1, column2, destination));
        }
        break;

        // catalog-lookup ICD-9|ICD-10 <column>
        case "catalog-lookup" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          if (!type.equalsIgnoreCase("ICD-9") && !type.equalsIgnoreCase("ICD-10-2016") &&
              !type.equalsIgnoreCase("ICD-10-2017")) {
            throw new IllegalArgumentException("Invalid ICD type - should be 9 (ICD-9) or 10 (ICD-10-2016 " +
                                                 "or ICD-10-2017).");
          } else {
            ICDCatalog catalog = new ICDCatalog(type);
            if (!catalog.configure()) {
              throw new DirectiveParseException(
                String.format("Failed to configure ICD StaticCatalog. Check with your administrator")
              );
            }
            steps.add(new CatalogLookup(lineno, directive, catalog, column));
          }
        }
        break;

        // table-lookup <column> <table>
        case "table-lookup" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String table = getNextToken(tokenizer, command, "table", lineno);
          steps.add(new TableLookup(lineno, directive, column, table));
        }
        break;

        // stemming <column>
        case "stemming" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new Stemming(lineno, directive, column));
        }
        break;

        // columns <sed>
        case "columns-replace" : {
          String sed = getNextToken(tokenizer, command, "sed-expression", lineno);
          steps.add(new ColumnsReplace(lineno, directive, sed));
        }
        break;

        // extract-regex-groups <column> <regex>
        case "extract-regex-groups" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String regex = getNextToken(tokenizer, command, "regex", lineno);
          steps.add(new ExtractRegexGroups(lineno, directive, column, regex));
        }
        break;

        // split-url <column>
        case "split-url" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          steps.add(new SplitURL(lineno, directive, column));
        }
        break;

        // cleanse-column-names
        case "cleanse-column-names" : {
          steps.add(new CleanseColumnNames(lineno, directive));
        }
        break;

        // change-column-case <upper|lower|uppercase|lowercase>
        case "change-column-case" : {
          String casing = getNextToken(tokenizer, command, "case", lineno);
          boolean toLower = false;
          if (casing == null || casing.isEmpty() || casing.equalsIgnoreCase("lower")
            || casing.equalsIgnoreCase("lowercase")) {
            toLower = true;
          }
          steps.add(new ChangeColCaseNames(lineno, directive, toLower));
        }
        break;

        // set-column <column> <expression>
        case "set-column" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String expr = getNextToken(tokenizer, "\n", command, "expression", lineno);
          steps.add(new SetColumn(lineno, directive, column, expr));
        }
        break;

        // encode <base32|base64|hex> <column>
        case "encode" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          type = type.toUpperCase();
          if (!type.equals("BASE64") && !type.equals("BASE32") && !type.equals("HEX")) {
            throw new DirectiveParseException(
              String.format("Type of encoding specified '%s' is not supported. Supports base64, base32 & hex.",
                            type)
            );
          }
          steps.add(new Encode(lineno, directive, Encode.Type.valueOf(type), column));
        }
        break;

        // decode <base32|base64|hex> <column>
        case "decode" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          type = type.toUpperCase();
          if (!type.equals("BASE64") && !type.equals("BASE32") && !type.equals("HEX")) {
            throw new DirectiveParseException(
              String.format("Type of decoding specified '%s' is not supported. Supports base64, base32 & hex.",
                            type)
            );
          }
          steps.add(new Decode(lineno, directive, Decode.Type.valueOf(type), column));
        }
        break;

        default:
          throw new DirectiveParseException(
            String.format("Unknown directive '%s' found in the directive at line %d", command, lineno)
          );
      }
      lineno++;
    }
    return steps;
  }

  // If there are more tokens, then it proceeds with parsing, else throws exception.
  private String getNextToken(StringTokenizer tokenizer, String directive,
                          String field, int lineno) throws DirectiveParseException {
    return getNextToken(tokenizer, null, directive, field, lineno, false);
  }

  private String getNextToken(StringTokenizer tokenizer, String delimiter,
                              String directive, String field, int lineno) throws DirectiveParseException {
    return getNextToken(tokenizer, delimiter, directive, field, lineno, false);
  }

  private String getNextToken(StringTokenizer tokenizer, String delimiter,
                          String directive, String field, int lineno, boolean optional)
    throws DirectiveParseException {
    String value = null;
    if (tokenizer.hasMoreTokens()) {
      if (delimiter == null) {
        value = tokenizer.nextToken().trim();
      } else {
        value = tokenizer.nextToken(delimiter).trim();
      }
    } else {
      if (!optional) {
        String usage = usageRegistry.getUsage(directive);
        throw new DirectiveParseException(
          String.format("Missing field '%s' at line number %d for directive <%s> (usage: %s)",
                        field, lineno, directive, usage)
        );
      }
    }
    return value;
  }

  /**
   * @return List of steps to executed in the order they are specified.
   * @throws ParseException throw in case of parsing exception of specification.
   */
  @Override
  public List<Step> getSteps() throws DirectiveParseException {
    return parse();
  }
}
