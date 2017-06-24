package co.cask.wrangler.parser;

import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.Token;
import co.cask.wrangler.registry.InternalRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Tests {@link RecipeCompiler}
 */
public class RecipeCompilerTest {

  @Test
  public void testSuccessCompilation() throws Exception {
    try {
      Compiler compiler = new RecipeCompiler();
      CompiledUnit units = compiler.compile(
          "parse-as-csv :body ' ' true;"
        + "set-column :abc, :edf;"
        + "send-to-error exp:{ window < 10 } ;"
        + "parse-as-simple-date :col 'yyyy-mm-dd' :col 'test' :col2,:col4,:col9 10 exp:{test < 10};"
      );

      InternalRegistry registry = new InternalRegistry();

      Iterator<TokenGroup> it = units.iterator();
      while(it.hasNext()) {
        TokenGroup tokens = it.next();
        Token token = tokens.get(0);
        String name = (String) token.value();
        Arguments arguments = new DefaultArguments(
          registry.definition(name),
          tokens
        );

        Class<?> directiveClass = registry.getDirective();
        UDD directive = (UDD) directiveClass.newInstance();
      }

      Assert.assertNotNull(units);
      Assert.assertEquals(4, units.size());
    } catch (CompileException e) {
      Assert.assertTrue(false);
    }
  }
}