# **PARSE-AS-TIMEDURATION Directive**

The `PARSE-AS-TIMEDURATION` directive converts a string representation of a time duration into a numeric value, expressed in nanoseconds.

## **Syntax**

parse-as-timeduration <column>

## **Usage Notes**
- This directive is used to parse string representations of time durations (e.g., "10ms", "5s", "1m", "2h", "1d") into their corresponding numeric values in nanoseconds.
- The column to be parsed must be of type `String`.
- If the column is `null` or already contains a numeric value, applying this directive will have no effect (it is a no-op).
  
## **How It Works**
The `PARSE-AS-TIMEDURATION` directive looks for time duration values in the following format:
- **ns** (nanoseconds), **us** (microseconds), **ms** (milliseconds), **s** (seconds), **m** (minutes), **h** (hours), **d** (days)

The string will be parsed and converted to its equivalent nanosecond value:
- "100ms" → 100 * 1,000,000 nanoseconds (100,000,000 nanoseconds)
- "2s" → 2 * 1,000,000,000 nanoseconds (2,000,000,000 nanoseconds)

If the string doesn't match a recognized time unit, an error will be thrown.

## **Examples**

### **Input**

"10ms"

### **Output**

10000000 (nanoseconds)

### **Explanation**
The string `"10ms"` is converted into 10 * 1,000,000 = 10,000,000 nanoseconds.

### **Input**
"2s"

### **Output**
2000000000 (nanoseconds)


### **Explanation**
The string `"2s"` is converted into 2 * 1,000,000,000 = 2,000,000,000 nanoseconds.

