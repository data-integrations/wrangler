# **PARSE-AS-BYTESIZE Directive**

The `PARSE-AS-BYTESIZE` directive converts a string representation of a byte size into a numeric value, expressed in bytes.

## **Syntax**

parse-as-bytesize <column>


## **Usage Notes**
- This directive is used to parse string representations of byte sizes (e.g., "10KB", "5MB", "1GB") into their corresponding numeric values in bytes.
- The column to be parsed must be of type `String`.
- If the column is `null` or already contains a numeric value, applying this directive will have no effect (it is a no-op).
  
## **How It Works**
The `PARSE-AS-BYTESIZE` directive looks for byte size values in the format:
- **B** (bytes), **KB** (kilobytes), **MB** (megabytes), **GB** (gigabytes), **TB** (terabytes), etc.

The string will be parsed and converted to its equivalent byte size:
- "10KB" → 10240 bytes
- "5MB" → 5 * 1024 * 1024 bytes (5,242,880 bytes)

If the string doesn't match a recognized byte size format, an error will be thrown.

## **Examples**

### **Input**
"10KB"


### **Output**
10240 (bytes)

### **Explanation**
The string `"10KB"` is converted into 10 * 1024 = 10240 bytes.
