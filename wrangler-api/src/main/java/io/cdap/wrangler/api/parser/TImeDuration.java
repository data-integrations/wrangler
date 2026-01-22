package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final long valueInMilliseconds;

    public TimeDuration(String value) {
        // Remove whitespace and lowercase everything
        value = value.trim().toLowerCase();

        String numberPart = value.replaceAll("[^0-9.]", "");
        String unit = value.replaceAll("[0-9.]", "");

        double number = Double.parseDouble(numberPart);

        switch (unit) {
            case "ms":
                valueInMilliseconds = (long) number;
                break;
            case "s":
                valueInMilliseconds = (long) (number * 1000);
                break;
            case "m":
                valueInMilliseconds = (long) (number * 60 * 1000);
                break;
            case "h":
                valueInMilliseconds = (long) (number * 60 * 60 * 1000);
                break;
            default:
                throw new IllegalArgumentException("Invalid time duration unit: " + unit);
        }
    }

    public long getMilliseconds() {
        return valueInMilliseconds;
    }

    @Override
    public String toString() {
        return String.valueOf(valueInMilliseconds) + " ms";
    }
}
