public class TimeDuration extends Token {
    private final long millis;

    public TimeDuration(String value) {
        super("TIME_DURATION", value);
        this.millis = parseMillis(value);
    }

    private long parseMillis(String value) {
        value = value.toLowerCase();
        double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
        if (value.endsWith("ns")) return (long) (number / 1_000_000.0);
        if (value.endsWith("ms")) return (long) number;
        if (value.endsWith("s")) return (long) (number * 1000);
        if (value.endsWith("m")) return (long) (number * 60 * 1000);
        if (value.endsWith("h")) return (long) (number * 3600 * 1000);
        return (long) number;
    }

    public long getMillis() {
        return millis;
    }
}
