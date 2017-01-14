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

package co.cask.wrangler.internal;

import co.cask.wrangler.api.Specification;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.steps.Columns;
import co.cask.wrangler.steps.CsvParser;
import co.cask.wrangler.steps.Drop;
import co.cask.wrangler.steps.Expression;
import co.cask.wrangler.steps.FormatDate;
import co.cask.wrangler.steps.IndexSplit;
import co.cask.wrangler.steps.Lower;
import co.cask.wrangler.steps.Mask;
import co.cask.wrangler.steps.Merge;
import co.cask.wrangler.steps.Quantization;
import co.cask.wrangler.steps.Rename;
import co.cask.wrangler.steps.RowConditionFilter;
import co.cask.wrangler.steps.RowRegexFilter;
import co.cask.wrangler.steps.Split;
import co.cask.wrangler.steps.TitleCase;
import co.cask.wrangler.steps.Upper;
import org.apache.commons.lang.StringEscapeUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Parses the DSL into specification containing steps for wrangling.
 *
 * Following are some of the commands and format that {@link TextSpecification}
 * will handle.
 *
 * <ul>
 *   <li>set format csv , true</li>
 *   <li>set columns a,b,c,d,e,f,g </li>
 *   <li>rename a first</li>
 *   <li>drop b</li>
 *   <li>merge d e h |</li>
 *   <li>uppercase h</li>
 *   <li>lowercase first</li>
 *   <li>titlecase c</li>
 *   <li>indexsplit h 1 4 splitcol</li>
 * </ul>
 */
public class TextSpecification implements Specification {
  static final char TAB = '\t';

  // directives for wrangling.
  private String[] directives;

  public TextSpecification(String[] directives) {
    this.directives = directives;
  }

  public TextSpecification(String directives) {
    this(directives.split("\n"));
  }

  /**
   * Parses the DSL to generate a sequence of steps to be executed by {@link co.cask.wrangler.api.Pipeline}.
   *
   * @return List of steps to be executed.
   * @throws ParseException
   */
  private List<Step> parse() throws ParseException {
    List<Step> steps = new ArrayList<>();

    // Split command by EOL
    int lineno = 1;

    // Iterate through each command and create necessary steps.
    for (String directive : directives) {
      StringTokenizer tokenizer = new StringTokenizer(directive, " ");
      String command = tokenizer.nextToken();

      switch (command) {
        case "set": {
          switch (tokenizer.nextToken()) {
            // set format [csv|json] <delimiter> <skip empty lines>
            case "format": {
              String format = tokenizer.nextToken();
              if (format.equalsIgnoreCase("csv")) {
                String delimStr = tokenizer.nextToken();
                char delimiter = delimStr.charAt(0);
                if (delimStr.startsWith("\\")) {
                  String unescapedStr = StringEscapeUtils.unescapeJava(delimStr);
                  if (unescapedStr == null) {
                    throw new IllegalArgumentException("Invalid delimiter for CSV Parser: " + delimStr);
                  }
                  delimiter = unescapedStr.charAt(0);
                }
                boolean ignoreEmptyLines = tokenizer.nextToken().equalsIgnoreCase("true");
                CsvParser.Options opt = new CsvParser.Options(delimiter, ignoreEmptyLines);
                steps.add(new CsvParser(lineno, directive, opt, STARTING_COLUMN, false));
                steps.add(new Drop(lineno, directive, STARTING_COLUMN));
              } else {
                throw new ParseException("Unknown format '" + format + "'", lineno);
              }
            }
            break;

            // set column <column-name> <jexl-expression>
            case "column": {
              String column = tokenizer.nextToken();
              String expr = tokenizer.nextToken("\n");
              steps.add(new Expression(lineno, directive, column, expr));
            }
            break;

            // set columns <name1, name2, ...>
            case "columns": {
              String cols[] = tokenizer.nextToken().split(",");
              steps.add(new Columns(lineno, directive, Arrays.asList(cols)));

            }
            break;
          }
        }
        break;

        // rename <source> <destination>
        case "rename": {
          String source = tokenizer.nextToken();
          String destination = tokenizer.nextToken();
          steps.add(new Rename(lineno, directive, source, destination));
        }
        break;

        // drop <column-name>
        case "drop": {
          steps.add(new Drop(lineno, directive, tokenizer.nextToken()));
        }
        break;

        // merge <col1> <col2> <destination-column-name> <delimiter>
        case "merge": {
          String col1 = tokenizer.nextToken();
          String col2 = tokenizer.nextToken();
          String dest = tokenizer.nextToken();
          String delimiter = tokenizer.nextToken();
          steps.add(new Merge(lineno, directive, col1, col2, dest, delimiter));
        }
        break;

        // uppercase <col>
        case "uppercase": {
          steps.add(new Upper(lineno, directive, tokenizer.nextToken()));
        }
        break;

        // lowercase <col>
        case "lowercase": {
          steps.add(new Lower(lineno, directive, tokenizer.nextToken()));
        }
        break;

        // titlecase <col>
        case "titlecase": {
          steps.add(new TitleCase(lineno, directive, tokenizer.nextToken()));
        }
        break;

        // indexsplit <source-column-name> <start> <end> <destination-column-name>
        case "indexsplit": {
          String source = tokenizer.nextToken();
          int start = Integer.parseInt(tokenizer.nextToken());
          int end = Integer.parseInt(tokenizer.nextToken());
          String destination = tokenizer.nextToken();
          steps.add(new IndexSplit(lineno, directive, source, start, end, destination));
        }
        break;

        // split <source-column-name> <delimiter> <new-column-1> <new-column-2>
        case "split": {
          String source = tokenizer.nextToken();
          String delimiter = tokenizer.nextToken();
          String firstCol = tokenizer.nextToken();
          String secondCol = tokenizer.nextToken();
          steps.add(new Split(lineno, directive, source, delimiter, firstCol, secondCol));
        }
        break;

        // filter-row-by-regex <column> <regex>
        case "filter-row-by-regex": {
          String column = tokenizer.nextToken();
          String pattern = tokenizer.nextToken("\n");
          steps.add(new RowRegexFilter(lineno, directive, column, pattern));
        }
        break;

        // filter-row-by-condition <column> <condition>
        case "filter-row-by-condition": {
          String condition = tokenizer.nextToken("\n");
          steps.add(new RowConditionFilter(lineno, directive, condition));
        }
        break;

        // mask-number <column> <mask-pattern>
        case "mask-number": {
          String column = tokenizer.nextToken();
          String mask = tokenizer.nextToken();
          steps.add(new Mask(lineno, directive, column, mask, Mask.MASK_NUMBER));
        }
        break;

        // mask-shuffle <column>
        case "mask-shuffle": {
          String column = tokenizer.nextToken();
          steps.add(new Mask(lineno, directive, column, Mask.MASK_SHUFFLE));
        }
        break;

        // format-date <column> <source-format> <destination-format>
        case "format-date": {
          String column = tokenizer.nextToken();
          String srcDatePattern = tokenizer.nextToken();
          String dstDatePattern = tokenizer.nextToken("\n");
          steps.add(new FormatDate(lineno, directive, column, srcDatePattern, dstDatePattern));
        }
        break;

        // format-unixtimestamp <column> <destination-format>
        case "format-unixtimestamp": {
          String column = tokenizer.nextToken();
          String dstDatePattern = tokenizer.nextToken("\n");
          steps.add(new FormatDate(lineno, directive, column, dstDatePattern));
        }
        break;

        // quantize <source-column> <destination-column> <[range1:range2)=value>,[<range1:range2=value>]*
        case "quantize": {
          String column1 = tokenizer.nextToken();
          String column2 = tokenizer.nextToken();
          String ranges = tokenizer.nextToken("\n");
          steps.add(new Quantization(lineno, directive, column1, column2, ranges));
        }
        break;

        default:
          throw new ParseException("Unknown command found in dsl", lineno);
      }
      lineno++;
    }
    return steps;
  }

  /**
   *
   * @return
   * @throws ParseException
   */
  @Override
  public List<Step> getSteps() throws ParseException {
    return parse();
  }
}

