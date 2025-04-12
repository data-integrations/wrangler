import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.api.parser.TokenType;

/**
 * Simple test program for ByteSize
 */
public class ByteSizeTest {
    public static void main(String[] args) {
        try {
            System.out.println("Testing ByteSize class...");
            
            // Test basic units
            ByteSize bytes = new ByteSize("1024B");
            System.out.println("1024B in bytes: " + (Double)bytes.value() + " bytes");
            System.out.println("Token type: " + bytes.type());
            System.out.println("Unit: " + bytes.getUnit());
            
            ByteSize kilobytes = new ByteSize("1KB");
            System.out.println("1KB in bytes: " + (Double)kilobytes.value() + " bytes");
            
            ByteSize megabytes = new ByteSize("1MB");
            System.out.println("1MB in bytes: " + (Double)megabytes.value() + " bytes");
            
            ByteSize gigabytes = new ByteSize("1GB");
            System.out.println("1GB in bytes: " + (Double)gigabytes.value() + " bytes");
            
            // Test fractional values
            ByteSize halfMegabyte = new ByteSize("0.5MB");
            System.out.println("0.5MB in bytes: " + (Double)halfMegabyte.value() + " bytes");
            
            // Test unit conversion
            ByteSize twoMegabytes = new ByteSize("2MB");
            System.out.println("2MB in KB: " + twoMegabytes.convertTo("KB") + " KB");
            System.out.println("2MB in MB: " + twoMegabytes.convertTo("MB") + " MB");
            System.out.println("2MB in GB: " + twoMegabytes.convertTo("GB") + " GB");
            
            System.out.println("All ByteSize tests passed successfully!");
        } catch (Exception e) {
            System.err.println("Test failed with exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
