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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.LazyNumber;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.parser.Bool;
import io.cdap.wrangler.api.parser.BoolList;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.ColumnNameList;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.NumericList;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TextList;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenDefinition;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class description here.
 */
public class MapArguments implements Arguments {
  private final Map<String, Token> tokens;
  private final int lineno;
  private final int columnno;
  private final String source;

  public MapArguments(UsageDefinition definition, TokenGroup group) throws DirectiveParseException {
    this.tokens = new HashMap<>();
    this.lineno = group.getSourceInfo().getLineNumber();
    this.columnno = group.getSourceInfo().getColumnNumber();
    this.source = group.getSourceInfo().getSource();
    int required = definition.getTokens().size() - definition.getOptionalTokensCount();
    if ((required > group.size() - 1) || ((group.size() - 1) > definition.getTokens().size())) {
      throw new DirectiveParseException(
        definition.getDirectiveName(), String.format("Improper usage of directive '%s', usage - '%s'",
                                                     definition.getDirectiveName(), definition.toString()));
    }

    List<TokenDefinition> specifications = definition.getTokens();
    Iterator<Token> it = group.iterator();
    int pos = 0;
    it.next(); // skip directive name.
    while (it.hasNext()) {
      Token token = it.next();
      while (pos < specifications.size()) {
        TokenDefinition specification = specifications.get(pos);
        if (!specification.optional()) {
          if (!specification.type().equals(token.type())) {
            if (specification.type() == TokenType.COLUMN_NAME_LIST && token.type() == TokenType.COLUMN_NAME) {
              List<String> values = new ArrayList<>();
              values.add(((ColumnName) token).value());
              tokens.put(specification.name(), new ColumnNameList(values));
              pos = pos + 1;
              break;
            } else if (specification.type() == TokenType.NUMERIC_LIST && token.type() == TokenType.NUMERIC) {
              List<LazyNumber> values = new ArrayList<>();
              values.add(((Numeric) token).value());
              tokens.put(specification.name(), new NumericList(values));
              pos = pos + 1;
              break;
            } else if (specification.type() == TokenType.BOOLEAN_LIST && token.type() == TokenType.BOOLEAN) {
              List<Boolean> values = new ArrayList<>();
              values.add(((Bool) token).value());
              tokens.put(specification.name(), new BoolList(values));
              pos = pos + 1;
              break;
            } else if (specification.type() == TokenType.TEXT_LIST && token.type() == TokenType.TEXT) {
              List<String> values = new ArrayList<>();
              values.add(((Text) token).value());
              tokens.put(specification.name(), new TextList(values));
              pos = pos + 1;
              break;
            } else {
              throw new DirectiveParseException(
                String.format("Expected argument '%s' to be of type '%s', but it is of type '%s' - %s",
                              specification.name(), specification.type().name(),
                              token.type().name(), group.getSourceInfo().toString())
              );
            }
          } else {
            tokens.put(specification.name(), token);
            pos = pos + 1;
            break;
          }
        } else {
          pos = pos + 1;
          if (specification.type().equals(token.type())) {
            tokens.put(specification.name(), token);
            break;
          }
        }
      }
    }
  }

  /**
   * Returns the number of tokens that are mapped to arguments.
   *
   * <p>The optional arguments specified during the <code>UsageDefinition</code>
   * are not included in the size if they are not present in the tokens parsed.</p>
   *
   * @return number of tokens parsed, excluding optional tokens if not present.
   */
  @Override
  public int size() {
    return tokens.size();
  }

  /**
   * This method checks if there exists a token named <code>name</code> registered
   * with this object.
   *
   * The <code>name</code> is expected to the same as specified in the <code>UsageDefinition</code>.
   * There are two reason why the <code>name</code> might not exists in this object :
   *
   * <ul>
   *   <li>When an token is defined to be optional, the user might not have specified the
   *   token, hence the token would not exist in the argument.</li>
   *   <li>User has specified invalid <code>name</code>.</li>
   * </ul>
   *
   * @param name associated with the token.
   * @return true if argument with name <code>name</code> exists, false otherwise.
   */
  @Override
  public boolean contains(String name) {
    return tokens.containsKey(name);
  }

  /**
   * This method returns the token {@code value} based on the {@code name}
   * specified in the argument. This method will attempt to convert the token
   * into the expected return type <code>T</code>.
   * <p>
   * <p>If the <code>name</code> doesn't exist in this object, then this
   * method is expected to return <code>null</code></p>
   *
   * @param name of the token to be retrieved.
   * @return object that extends <code>Token</code>.
   */
  @Override
  public <T extends Token> T value(String name) {
    return (T) tokens.get(name);
  }

  /**
   * Each token is defined as one of the types defined in the class {@link TokenType}.
   * When the directive is parsed into token, the type of the token is passed through.
   *
   * @param name associated with the token.
   * @return <code>TokenType</code> associated with argument <code>name</code>, else null.
   */
  @Override
  public TokenType type(String name) {
    return tokens.get(name).type();
  }

  /**
   * Returns the source line number these arguments were parsed from.
   *
   * @return the source line number.
   */
  @Override
  public int line() {
    return lineno;
  }

  /**
   * Returns the source column number these arguments were parsed from.
   * <p>It takes the start position of the directive as the column number.</p>
   *
   * @return the start of the column number for the start of the directive
   * these arguments contain.
   */
  @Override
  public int column() {
    return columnno;
  }

  /**
   * This method returns the original source line of the directive as specified
   * the user. It returns the <code>String</code> representation of the directive.
   *
   * @return <code>String</code> object representing the original directive
   * as specified by the user.
   */
  @Override
  public String source() {
    return source;
  }

  /**
   * Returns <code>JsonElement</code> representation of this object.
   *
   * @return an instance of <code>JsonElement</code>object representing all the
   * named tokens held within this object.
   */
  @Override
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    JsonObject arguments = new JsonObject();
    for (Map.Entry<String, Token> entry : tokens.entrySet()) {
      arguments.add(entry.getKey(), entry.getValue().toJson());
    }
    object.addProperty("line", lineno);
    object.addProperty("column", columnno);
    object.addProperty("source", source);
    object.add("arguments", arguments);
    return object;
  }
}
