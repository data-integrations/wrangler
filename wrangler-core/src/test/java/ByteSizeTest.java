import org.junit.Test;
import org.junit.Assert;
import io.cdap.wrangler.api.parser.ByteSize;

public class ByteSizeTest {
    @Test
public void testByteSizeParsing() {
  Assert.assertEquals(10240, new ByteSize("10KB").getBytes(), 0.001);
  Assert.assertEquals(1572864, new ByteSize("1.5MB").getBytes(), 0.001);
}

}
