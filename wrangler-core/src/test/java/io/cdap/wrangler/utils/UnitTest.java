package io.cdap.wrangler.util;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class UnitTest {

  @Test
  public void testByteSizeParsing() {
    ByteSize bs1 = new ByteSize("10KB");
    Assert.assertEquals(10 * 1024, bs1.getBytes());
    
    ByteSize bs2 = new ByteSize("1.5MB");
    Assert.assertEquals((long)(1.5 * 1024 * 1024), bs2.getBytes());
    
    ByteSize bs3 = new ByteSize("1024B");
    Assert.assertEquals(1024, bs3.getBytes());
  }

  @Test
  public void testTimeDurationParsing() {
    TimeDuration td1 = new TimeDuration("500ms");
    Assert.assertEquals(500, td1.getMilliseconds());
    
    TimeDuration td2 = new TimeDuration("2.1s");
    Assert.assertEquals((long)(2.1 * 1000), td2.getMilliseconds());
    
    TimeDuration td3 = new TimeDuration("5m");
    Assert.assertEquals(5 * 60 * 1000, td3.getMilliseconds());
  }
}
