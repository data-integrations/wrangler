package co.cask.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link RecipeCompiler}
 */
public class RecipeCompilerTest {

  @Test
  public void testSuccessCompilation() throws Exception {
    try {
      Compiler compiler = new RecipeCompiler();
      CompiledUnit tokens = compiler.compile(
          "parse-as-csv :body ' ' true;"
        + "set-column :abc, :edf;"
        + "send-to-error exp:{ window < 10 } ;"
        + "parse-as-simple-date :col 'yyyy-mm-dd' :col 'test' :col2,:col4,:col9 10 exp:{test < 10};"
      );
      Assert.assertNotNull(tokens);
      Assert.assertEquals(4, tokens.size());
    } catch (CompileException e) {
      Assert.assertTrue(false);
    }
  }
}