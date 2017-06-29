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

import co.cask.wrangler.api.parser.SyntaxError;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class description here.
 */
public final class SyntaxErrorListener extends BaseErrorListener {
  private List<SyntaxError> errors = new ArrayList<>();

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer,
                          Object offendingSymbol,
                          int line, int charPositionInLine,
                          String msg,
                          RecognitionException e) {


    msg = msg.substring(0,1).toUpperCase() + msg.substring(1);

    String symbolText = "";
    if (offendingSymbol instanceof CommonToken) {
      CommonToken symbol = (CommonToken) offendingSymbol;
      String charstream = symbol.getTokenSource().getInputStream().toString();
      String[] lines = charstream.split("\n");
      symbolText = String.format("Error at token '" + symbol.getText() + "', source : %s", lines[line-1]);
    }
    msg = !symbolText.isEmpty() ? symbolText + ", " + msg : msg;
    msg = String.format("line %d:%d %s", line, charPositionInLine, msg);
    errors.add(new SyntaxError(line, charPositionInLine, msg, symbolText));
  }

  public boolean hasErrors() {
    return errors.size() > 0;
  }

  public Iterator<SyntaxError> iterator() {
    return errors.iterator();
  }

}
