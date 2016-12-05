/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.wrangler.steps.Lower;
import co.cask.wrangler.steps.Rename;
import co.cask.wrangler.steps.TitleCase;
import co.cask.wrangler.steps.Upper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parses the DSL into specification containing steps for wrangling.
 *
 * Following are some of the commands and format that {@link SimpleSpecification}
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
public class SimpleSpecification implements Specification {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleSpecification.class);

  private List<Step> steps = new ArrayList<>();
  private String dsl;

  public SimpleSpecification(String dsl) {
    this.dsl = dsl;
    try {
      parse();
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  private void parse() throws ParseException {
    String[] lines = dsl.split("\n");
    int lineno = 1;
    for (String line : lines) {
      line = line.trim().replaceAll(" +"," ");
      String[] cmds = line.split(" ",0);
      switch (cmds[0]) {
        case "set":
          switch (cmds[1]) {
            case "format":
              if (cmds[2].equalsIgnoreCase("csv")) {
                boolean ignoreEmptyLines = false;
                if (cmds[3].equalsIgnoreCase("true")) {
                  ignoreEmptyLines = true;
                }
                CsvParser.Options options = new CsvParser.Options(cmds[3].charAt(0), ignoreEmptyLines);
                steps.add(new CsvParser(options, "__col", false));
                steps.add(new Drop("__col"));
              } else {
                throw new ParseException("Unknown format " + cmds[3], lineno);
              }
              break;

            case "columns":
              String cols[] = cmds[2].split(",");
              steps.add(new Columns(Arrays.asList(cols)));
              break;
          }
          break;

        case "rename":
          steps.add(new Rename(cmds[1], cmds[2]));
          break;

        case "drop":
          steps.add(new Drop(cmds[1]));
          break;

        case "merge":
          LOG.info("Merge command");
          break;

        case "uppercase":
          steps.add(new Upper(cmds[2]));
          break;

        case "lowercase":
          steps.add(new Lower(cmds[2]));
          break;

        case "titlecase":
          steps.add(new TitleCase(cmds[2]));
          break;

        case "indexsplit":
          LOG.info("IndexSplit command");
          break;

        default:
          throw new ParseException("Unknown command found in dsl", lineno);
      }
      lineno++;
    }
  }

  @Override
  public List<Step> getSteps() {
    return steps;
  }
}
