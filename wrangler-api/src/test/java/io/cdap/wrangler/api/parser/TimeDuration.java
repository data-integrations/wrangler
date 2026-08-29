package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final double value;
    private final String unit;

    public TimeDuration(String str) {
        super(str);
        str = str.trim().toLowerCase();
        this.unit = str.replaceAll("[^a-z]", "");
        this.value = Double.parseDouble(str.replaceAll("[^0-9.]", ""));
    }

    public long getMilliseconds() {
        switch (unit) {
            case "s": return (long)(value * 1000);
            case "min": return (long)(value * 60000);
            case "h": return (long)(value * 3600000);
            default: return (long)value;
        }
    }
}
