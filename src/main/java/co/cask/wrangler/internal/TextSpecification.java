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
import co.cask.wrangler.steps.FormatDate;
import co.cask.wrangler.steps.IndexSplit;
import co.cask.wrangler.steps.Lower;
import co.cask.wrangler.steps.Mask;
import co.cask.wrangler.steps.Merge;
import co.cask.wrangler.steps.Rename;
import co.cask.wrangler.steps.RowRegexFilter;
import co.cask.wrangler.steps.Split;
import co.cask.wrangler.steps.TitleCase;
import co.cask.wrangler.steps.Upper;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
  private static final Logger LOG = LoggerFactory.getLogger(TextSpecification.class);
  static final char TAB = '\t';

  // DSL for wrangling.
  private String dsl;

  public TextSpecification(String dsl) {
    this.dsl = dsl;
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
    String[] lines = dsl.split("\n");
    int lineno = 1;

    // Iterate through each command and create necessary steps.
    for (String line : lines) {
      line = line.trim().replaceAll(" +"," ");
      String[] options = line.split(" ",0);
      String command = options[0];
      String qualifier = options[1];

      switch (command) {
        case "set":
          switch (qualifier) {
            // set format [csv|json] <delimiter> <skip empty lines>
            case "format":
              if (options[2].equalsIgnoreCase("csv")) {
                boolean ignoreEmptyLines = false;
                if (options[3].equalsIgnoreCase("true")) {
                  ignoreEmptyLines = true;
                }
                char delimiter = options[3].charAt(0);
                if (options[3].startsWith("\\")) {
                  String unescapedStr = StringEscapeUtils.unescapeJava(options[3]);
                  if (unescapedStr == null) {
                    throw new IllegalArgumentException("Invalid delimiter for CSV Parser: " + options[3]);
                  }
                  delimiter = unescapedStr.charAt(0);
                }
                CsvParser.Options opt = new CsvParser.Options(delimiter, ignoreEmptyLines);
                steps.add(new CsvParser(lineno, line, opt, STARTING_COLUMN, false));
                steps.add(new Drop(lineno, line, STARTING_COLUMN));
              } else {
                throw new ParseException("Unknown format " + options[3], lineno);
              }
              break;

            // set columns <name1, name2, ...>
            case "columns":
              String cols[] = options[2].split(",");
              steps.add(new Columns(lineno, line, Arrays.asList(cols)));
              break;
          }
          break;

        // rename <source> <destination>
        case "rename":
          steps.add(new Rename(lineno, line, qualifier, options[2]));
          break;

        // drop <column-name>
        case "drop":
          steps.add(new Drop(lineno, line, qualifier));
          break;

        // merge <col1> <col2> <destination-column-name> <delimiter>
        case "merge":
          steps.add(new Merge(lineno, line, qualifier, options[1], options[2], options[3]));
          break;

        // uppercase <col>
        case "uppercase":
          steps.add(new Upper(lineno, line, qualifier));
          break;

        // lowercase <col>
        case "lowercase":
          steps.add(new Lower(lineno, line, options[2]));
          break;

        // titlecase <col>
        case "titlecase":
          steps.add(new TitleCase(lineno, line, options[2]));
          break;

        // indexsplit <source-column-name> <start> <end> <destination-column-name>
        case "indexsplit":
          steps.add(new IndexSplit(lineno, line, qualifier, Integer.valueOf(options[2]),
                                   Integer.valueOf(options[3]), options[4]));
          break;

        // split <source-column-name> <delimiter> <new-column-1> <new-column-2>
        case "split":
          steps.add(new Split(lineno, line, qualifier, options[2], options[3], options[4]));
          break;

        // filter-row-by-regex <column> <regex>
        case "filter-row-by-regex":
          steps.add(new RowRegexFilter(lineno, line, qualifier, options[2]));
          break;

        // mask-number <column> <mask-pattern>
        case "mask-number":
          steps.add(new Mask(lineno, line, qualifier, options[2], Mask.MASK_NUMBER));
          break;

        // mask-shuffle <column>
        case "mask-shuffle":
          steps.add(new Mask(lineno, line, qualifier, null, Mask.MASK_SHUFFLE));
          break;

        // format-date <column> <source-format> <destination-format>
        case "format-date":
          steps.add(new FormatDate(lineno, line, qualifier, options[2], options[3]));
          break;

        // format-unixtimestamp <column> <destination-format>
        case "format-unixtimestamp":
          steps.add(new FormatDate(lineno, line, qualifier, options[2]));

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

