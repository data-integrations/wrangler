// ByteSize.java
package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private final long bytes;
    
    public ByteSize(String value) {
        super(TokenType.BYTE_SIZE);
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String value) {
        String num = value.replaceAll("[^0-9]", "");
        String unit = value.replaceAll("[0-9]", "").toUpperCase();
        
        long multiplier = 1;
        switch (unit) {
            case "KB": multiplier = 1024; break;
            case "MB": multiplier = 1024 * 1024; break;
            case "GB": multiplier = 1024 * 1024 * 1024; break;
            case "TB": multiplier = 1024L * 1024 * 1024 * 1024; break;
        }
        
        return Long.parseLong(num) * multiplier;
    }

    public long getBytes() { return bytes; }
    public double getKB() { return bytes / 1024.0; }
    public double getMB() { return bytes / (1024.0 * 1024); }
}