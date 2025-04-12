package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for ByteSize parser.
 */
public class ByteSizeTest {
    
    @Test
    public void testBasicParsing() throws SyntaxError {
        ByteSize bytes = new ByteSize("1024B");
        Assert.assertEquals(1024.0, (double) bytes.value(), 0.01);
        Assert.assertEquals(TokenType.BYTE_SIZE, bytes.type());
        Assert.assertEquals("B", bytes.getUnit());
        
        ByteSize kilobytes = new ByteSize("1KB");
        Assert.assertEquals(1024.0, (double) kilobytes.value(), 0.01);
        
        ByteSize megabytes = new ByteSize("1MB");
        Assert.assertEquals(1048576.0, (double) megabytes.value(), 0.01);
        
        ByteSize gigabytes = new ByteSize("1GB");
        Assert.assertEquals(1073741824.0, (double) gigabytes.value(), 0.01);
    }
    
    @Test
    public void testFractionalValues() throws SyntaxError {
        ByteSize halfMegabyte = new ByteSize("0.5MB");
        Assert.assertEquals(524288.0, (double) halfMegabyte.value(), 0.01);
    }
    
    @Test
    public void testUnitConversion() throws SyntaxError {
        ByteSize twoMegabytes = new ByteSize("2MB");
        Assert.assertEquals(2048.0, twoMegabytes.convertTo("KB"), 0.01);
        Assert.assertEquals(2.0, twoMegabytes.convertTo("MB"), 0.01);
        Assert.assertEquals(0.001953125, twoMegabytes.convertTo("GB"), 0.00000001);
    }
    
    @Test
    public void testWhitespaceHandling() throws SyntaxError {
        ByteSize noSpace = new ByteSize("10MB");
        ByteSize withSpace = new ByteSize("10 MB");
        Assert.assertEquals((double) noSpace.value(), (double) withSpace.value(), 0.01);
    }
    
    @Test(expected = SyntaxError.class)
    public void testInvalidFormat1() throws SyntaxError {
        // Format should be number followed by unit, not unit followed by number
        new ByteSize("MB10");
    }
    
    @Test(expected = SyntaxError.class)
    public void testInvalidUnit() throws SyntaxError {
        // ZB is not a supported unit
        new ByteSize("10ZB");
    }
    
    @Test(expected = SyntaxError.class)
    public void testMissingUnit() throws SyntaxError {
        // Unit is required
        new ByteSize("1024");
    }
}
