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

package io.cdap.directives.currency;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.apache.commons.lang3.LocaleUtils;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

/**
 * A directive for taking difference in Dates.
 */
@Plugin(type = Directive.TYPE)
@Name(FormatAsCurrency.NAME)
@Categories(categories = {"currency"})
@Description("Formats a number as currency using the locale specified. Default locale is en_US.")
public class FormatAsCurrency implements Directive, Lineage {
  public static final String NAME = "format-as-currency";
  private String source;
  private String destination;
  private String locale;
  private NumberFormat format;
  private Locale lcl;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("locale", TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.source = ((ColumnName) args.value("source")).value();
    this.destination = ((ColumnName) args.value("destination")).value();

    if (args.contains("locale")) {
      this.locale = ((Text) args.value("locale")).value();
    } else {
      this.locale = "en_US";
    }

    this.lcl = LocaleUtils.toLocale(locale);
    this.format = NumberFormat.getCurrencyInstance(lcl);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      int idx = row.find(source);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object == null || !(object instanceof Double)) {
          continue;
        }
        Double value = (Double) object;
        String format = this.format.format(value);
        row.addOrSet(destination, format);
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Formatted column '%s' to currency locale '%s' into '%s'", source, destination, locale)
      .conditional(source, destination)
      .build();
  }
}
