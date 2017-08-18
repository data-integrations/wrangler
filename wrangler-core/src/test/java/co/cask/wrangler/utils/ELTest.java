package co.cask.wrangler.utils;

import co.cask.wrangler.expression.EL;
import co.cask.wrangler.expression.ELContext;
import co.cask.wrangler.expression.ELException;
import co.cask.wrangler.expression.ELResult;
import org.junit.Assert;
import org.junit.Test;

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
}