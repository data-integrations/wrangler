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

import io.cdap.wrangler.api.parser.SyntaxError;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class description here.
 */
public final class SyntaxErrorListener extends BaseErrorListener {
  public int lastError = -1;
  private List<SyntaxError> errors = new ArrayList<>();

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                          String msg, RecognitionException e) {

    Parser parser = (Parser) recognizer;
    String name = parser.getSourceName();
    TokenStream tokens = parser.getInputStream();

    Token offSymbol = (Token) offendingSymbol;
    int thisError = offSymbol.getTokenIndex();

    String source = "<unknown>";
    if (offSymbol != null) {
      String charstream = offSymbol.getTokenSource().getInputStream().toString();
      String[] lines = charstream.split("\n");
      source = lines[line - 1];
    }

    if (offSymbol.getType() == -1 && thisError == tokens.size() - 1) {
      if (e != null) {
        if (e instanceof NoViableAltException) {
          msg = "unexpected token found '" + ((NoViableAltException) e).getStartToken().getText() + "'";
        }
      }
      String message = "At line " + line + ":" + charPositionInLine +  ": " + msg;
      errors.add(new SyntaxError(line, charPositionInLine, message, source));
      return;
    }

    String offSymName = DirectivesLexer.VOCABULARY.getDisplayName(offSymbol.getType());
    String message = "At line " + line + ":" + charPositionInLine + " at " + offSymName.toLowerCase() + ": " + msg;

//    StringBuilder sb = new StringBuilder(message);
//    sb.append(", alternatives = {");
//    for (int idx = lastError + 1; idx <= thisError; idx++) {
//      Token token = tokens.get(idx);
//      if (token.getChannel() != Token.HIDDEN_CHANNEL) {
//        sb.append(token.getText()).append(",");
//      }
//    }
//    sb.append("}");
    lastError = thisError;
    errors.add(new SyntaxError(line, charPositionInLine, message, source));
  }

  public boolean hasErrors() {
    return errors.size() > 0;
  }

  public Iterator<SyntaxError> iterator() {
    return errors.iterator();
  }

}
