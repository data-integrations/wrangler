package co.cask.wrangler.expression;

import co.cask.wrangler.expression.EL;
import co.cask.wrangler.expression.ELContext;
import co.cask.wrangler.expression.ELException;
import co.cask.wrangler.expression.ELResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link EL}
 */
public class ELTest {

  @Test
  public void testBasicFunctionality() throws Exception {
    EL el = new EL(new EL.DefaultFunctions());
    el.compile("a + b");
    ELResult execute = el.execute(new ELContext().add("a", 1).add("b", 2));
    Assert.assertNotNull(execute);
    Assert.assertEquals(new Integer(3), execute.getInteger());
    Assert.assertEquals(true, el.variables().contains("a"));
    Assert.assertEquals(false, el.variables().contains("c"));
  }

  @Test(expected = ELException.class)
  public void testUndefinedVariableException() throws Exception {
    EL el = new EL(new EL.DefaultFunctions());
    el.compile("a + b + c");
    el.execute(new ELContext().add("a", 1).add("b", 2));
  }

  @Test
  public void testArrays() throws Exception {
    EL el = new EL(new EL.DefaultFunctions());
    el.compile("runtime['map'] > token['ABC.EDFG']['input'] " +
                 "&& math:max(toDouble(runtime['map']), toDouble(token['ABC.EDFG']['input'])) > 9");

    Map<String, Object> runtime = new HashMap<>();
    runtime.put("map", "10");
    ELContext ctx = new ELContext();
    ctx.add("runtime", runtime);

    Map<String, Map<String, Object>> token = new HashMap<>();
    Map<String, Object> stage1 = new HashMap<>();
    stage1.put("input", "1");
    token.put("ABC.EDFG", stage1);
    ctx.add("token", token);
    ELResult execute = el.execute(ctx);
    Assert.assertEquals(true, execute.getBoolean());
  }
}