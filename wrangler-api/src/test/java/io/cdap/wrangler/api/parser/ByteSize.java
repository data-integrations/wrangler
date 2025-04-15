package io.cdap.wrangler.api.parser;

public class ByteSize implements Token {
    private final long bytes;

    public ByteSize(String input) {
        input = input.toUpperCase().trim();
        if (input.endsWith("KB")) bytes = (long)(Double.parseDouble(input.replace("KB", "")) * 1024);
        else if (input.endsWith("MB")) bytes = (long)(Double.parseDouble(input.replace("MB", "")) * 1024 * 1024);
        else if (input.endsWith("GB")) bytes = (long)(Double.parseDouble(input.replace("GB", "")) * 1024 * 1024 * 1024);
        else if (input.endsWith("TB")) bytes = (long)(Double.parseDouble(input.replace("TB", "")) * 1024L * 1024 * 1024 * 1024);
        else if (input.endsWith("B")) bytes = Long.parseLong(input.replace("B", ""));
        else throw new IllegalArgumentException("Invalid byte size format: " + input);
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return bytes + "B";
    }
}
