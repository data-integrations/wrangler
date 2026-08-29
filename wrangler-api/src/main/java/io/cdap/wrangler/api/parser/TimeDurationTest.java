

 package io.cdap.wrangler.api.parser;

 import org.junit.Assert;
 import org.junit.Test;
 
 public class TimeDurationTest {
   @Test
   public void testBasicParsing() throws Exception {
     TimeDuration duration = new TimeDuration("1s");
     Assert.assertEquals(1000000000L, duration.getNanos());
     Assert.assertEquals(1000.0, duration.getMillis(), 0.001);
     Assert.assertEquals(1.0, duration.getSeconds(), 0.001);
   }
 
   @Test
   public void testDifferentUnits() throws Exception {
     Assert.assertEquals(1L, new TimeDuration("1ns").getNanos());
     Assert.assertEquals(1000000L, new TimeDuration("1ms").getNanos());
     Assert.assertEquals(1000000000L, new TimeDuration("1s").getNanos());
   }
 
   @Test
   public void testDecimalValues() throws Exception {
     Assert.assertEquals(500000L, new TimeDuration("0.5ms").getNanos());
     Assert.assertEquals(1500000L, new TimeDuration("1.5ms").getNanos());
     Assert.assertEquals(500000000L, new TimeDuration("0.5s").getNanos());
   }
 
   @Test
   public void testCaseInsensitive() throws Exception {
     Assert.assertEquals(1L, new TimeDuration("1NS").getNanos());
     Assert.assertEquals(1000000L, new TimeDuration("1MS").getNanos());
     Assert.assertEquals(1000000000L, new TimeDuration("1S").getNanos());
   }
 
   @Test(expected = IllegalArgumentException.class)
   public void testInvalidFormat() throws Exception {
     new TimeDuration("invalid");
   }
 
   @Test(expected = IllegalArgumentException.class)
   public void testInvalidUnit() throws Exception {
     new TimeDuration("1xs");
   }
 
   @Test(expected = IllegalArgumentException.class)
   public void testNegativeValue() throws Exception {
     new TimeDuration("-1s");
   }
 }