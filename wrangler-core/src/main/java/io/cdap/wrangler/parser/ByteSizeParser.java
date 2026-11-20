package io.cdap.wrangler.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteSizeParser {

    private static final Pattern PATTERN = Pattern.compile("(\\d+(\\.\\d+)?)(\\s*)([KMGTP]?B)", Pattern.CASE_INSENSITIVE);

    public static long parse(String input) throws IllegalArgumentException {
        Matcher matcher = PATTERN.matcher(input.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size format: " + input);
        }

        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(4).toUpperCase();

        switch (unit) {
            case "B":
                return (long) value;
            case "KB":
                return (long) (value * 1024);
            case "MB":
                return (long) (value * 1024 * 1024);
            case "GB":
                return (long) (value * 1024 * 1024 * 1024);
            case "TB":
                return (long) (value * 1024L * 1024L * 1024L * 1024L);
            case "PB":
                return (long) (value * 1024L * 1024L * 1024L * 1024L * 1024L);
            default:
                throw new IllegalArgumentException("Unknown unit: " + unit);
        }
    }
}
