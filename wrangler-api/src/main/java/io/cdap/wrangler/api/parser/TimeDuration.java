public class TimeDuration extends Token {
    private final long milliseconds;

    public TimeDuration(String value) {
        super(value);
        this.milliseconds = parseTime(value);
    }

    private long parseTime(String val) {
        String unit = val.replaceAll("[0-9.]", "");
        double num = Double.parseDouble(val.replaceAll("[^0-9.]", ""));
        switch (unit) {
            case "ms": return (long) num;
            case "s": return (long) (num * 1000);
            case "m": return (long) (num * 60 * 1000);
            case "h": return (long) (num * 3600 * 1000);
            case "d": return (long) (num * 86400 * 1000);
            default: throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }

    public long getMilliseconds() {
        return milliseconds;
    }
}
