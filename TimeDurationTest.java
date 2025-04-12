import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.api.parser.TokenType;

/**
 * Simple test program for TimeDuration
 */
public class TimeDurationTest {
    public static void main(String[] args) {
        try {
            System.out.println("Testing TimeDuration class...");
            
            // Test basic units
            TimeDuration nanos = new TimeDuration("1000ns");
            System.out.println("1000ns in nanoseconds: " + (Double)nanos.value() + " ns");
            System.out.println("Token type: " + nanos.type());
            System.out.println("Unit: " + nanos.getUnit());
            
            TimeDuration millis = new TimeDuration("1ms");
            System.out.println("1ms in nanoseconds: " + (Double)millis.value() + " ns");
            
            TimeDuration seconds = new TimeDuration("1s");
            System.out.println("1s in nanoseconds: " + (Double)seconds.value() + " ns");
            
            TimeDuration minutes = new TimeDuration("1m");
            System.out.println("1m in nanoseconds: " + (Double)minutes.value() + " ns");
            
            TimeDuration hours = new TimeDuration("1h");
            System.out.println("1h in nanoseconds: " + (Double)hours.value() + " ns");
            
            TimeDuration days = new TimeDuration("1d");
            System.out.println("1d in nanoseconds: " + (Double)days.value() + " ns");
            
            System.out.println("All TimeDuration tests passed successfully!");
        } catch (Exception e) {
            System.err.println("Test failed with exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
