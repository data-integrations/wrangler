package co.cask.wrangler.parser;

import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.parser.SyntaxError;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class description here.
 */
public final class SyntaxErrorListener extends BaseErrorListener {
  private List<SyntaxError> errors = new ArrayList<>();

  public void syntaxError(Recognizer<?, ?> recognizer,
                          Object offendingSymbol,
                          int line, int charPositionInLine,
                          String msg,
                          RecognitionException e)
  {
    Pair<String, String> underline = underlineError(recognizer, (Token) offendingSymbol,
                                                           line, charPositionInLine);
    errors.add(new SyntaxError(line, charPositionInLine, msg, underline.getFirst(), underline.getSecond()));
  }

  public boolean hasErrors() {
    return errors.size() > 0;
  }

  public Iterator<SyntaxError> iterator() {
    return errors.iterator();
  }

  protected Pair<String, String> underlineError(Recognizer recognizer,
                                                Token offendingToken, int line,
                                                int charPositionInLine) {
    CommonTokenStream tokens =
      (CommonTokenStream)recognizer.getInputStream();
    String input = tokens.getTokenSource().getInputStream().toString();
    String[] lines = input.split("\n");
    String errorLine = lines[line - 1];

    StringBuilder sb = new StringBuilder();
    for (int i=0; i<charPositionInLine; i++) {
      sb.append("_");
    }
    int start = offendingToken.getStartIndex();
    int stop = offendingToken.getStopIndex();
    if ( start>=0 && stop>=0 ) {
      if (start > stop) {
        sb.append("^");
      } else {
        for (int i=start; i<=stop; i++) {
          sb.append("^");
        }
      }
    }
    return new Pair<>(errorLine, sb.toString());
  }
}
