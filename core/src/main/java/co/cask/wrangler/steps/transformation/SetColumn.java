/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;

/**
 * A Wrangler step for apply an expression to store the result in a column.
 *
 * The expressions are specified in JEXL format (http://commons.apache.org/proper/commons-jexl/reference/syntax.html)
 * Directive is response for executing only one expression for each {@link Row} record that is
 * being passed. The result of expression either adds a new column or replaces the value of
 * the existing column.
 *
 * <p>
 *   Directive step = new Expression(lineno, directive, column, "if (age > 24 ) { 'adult' } else { 'teen' }");
 * </p>
 */
@Plugin(type = "udd")
@Name("set-column")
@Usage("set-column <column> <expression>")
@Description("Sets a column the result of expression execution.")
public class SetColumn extends Expression {
  public SetColumn(int lineno, String detail, String column, String expression) {
    super(lineno, detail, column, expression);
  }
}


