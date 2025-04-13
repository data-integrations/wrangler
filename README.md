Absolutely! Here's the **entire enhancement process rewritten in full `README.md` Markdown syntax**, including **all the steps** from setup to testing and completion. You can copy and paste this directly into your `README.md` file.

---

```markdown
# Wrangler Enhancement: Byte Size and Time Duration Handling

## 📝 Overview

This enhancement adds native support for **Byte Size** (e.g., `10MB`, `2.5GB`) and **Time Duration** (e.g., `300ms`, `2h`) in the Wrangler framework. It includes grammar support, token parsing, API enhancements, directive implementation, and full testing coverage. This allows new capabilities in parsing, conversion, and aggregation using the `aggregate-stats` directive.

---

## 🚀 Features

- ✅ Byte Size & Time Duration token parsing via ANTLR
- ✅ New `ByteSize` and `TimeDuration` classes with unit conversions
- ✅ Support for new tokens in `RecipeVisitor`
- ✅ Enhanced `aggregate-stats` directive with support for byte/time values
- ✅ Unit and integration testing of all new functionality

---

## 🛠️ Setup Instructions

### 1. Fork & Clone Wrangler

1. Fork the Wrangler repository: [https://github.com/BlackSnow5120](https://github.com/BlackSnow5120)
2. Clone and build the project:
   ```bash
   git clone https://github.com/BlackSnow5120/wrangler.git
   cd wrangler
   mvn clean install
   ```

---

## 🔧 Enhancement Steps

### 2. Modify ANTLR Grammar

#### File: `Directives.g4`

##### ➕ Add Lexer Tokens

```antlr
BYTE_SIZE     : [0-9]+ ('.' [0-9]+)? ( 'B' | 'KB' | 'MB' | 'GB' | 'TB' );
TIME_DURATION : [0-9]+ ('.' [0-9]+)? ( 'ms' | 's' | 'm' | 'h' | 'd' );
```

##### 🔁 Regenerate the ANTLR Parser

```bash
mvn clean compile
```

---

### 3. API Enhancements (`wrangler-api`)

#### ➕ Create `ByteSize.java`

```java
public class ByteSize extends Token {
    private String value;

    public ByteSize(String value) {
        this.value = value;
    }

    public long getBytes() {
        // Conversion logic (e.g., 10MB -> 10485760)
    }
}
```

#### ➕ Create `TimeDuration.java`

```java
public class TimeDuration extends Token {
    private String value;

    public TimeDuration(String value) {
        this.value = value;
    }

    public long getMillis() {
        // Conversion logic (e.g., 5s -> 5000)
    }
}
```

#### 🧩 Add to `TokenType` enum

```java
BYTE_SIZE,
TIME_DURATION
```

---

### 4. Core Parser Enhancements (`wrangler-core`)

#### ✏️ Modify `RecipeVisitor.java`

```java
public class RecipeVisitor extends AbstractVisitor {
    public Object visitByteSizeArg(ByteSize byteSize) {
        // Process ByteSize
    }

    public Object visitTimeDurationArg(TimeDuration timeDuration) {
        // Process TimeDuration
    }
}
```

#### ➕ Add ByteSize and TimeDuration tokens to `TokenGroup` for aggregation

---

### 5. Implement `aggregate-stats` Directive

#### 🔄 Accept Byte/Time Columns

```java
String[] recipe = new String[] {
    "aggregate-stats :data_transfer_size :response_time sum_size_mb sum_time_sec"
};
```

#### 🧠 Accumulate & Convert

```java
executorContext.store.addAggregatedValue("sum_size_mb", sizeInMB);
executorContext.store.addAggregatedValue("sum_time_sec", timeInSeconds);
```

#### ✅ Output Aggregated Row with Metrics

- `sum`
- `average`
- `median`
- etc.

---

### 6. 🧪 Testing

#### ✅ Unit Tests

```java
Assert.assertEquals(expectedBytes, byteSize.getBytes());
Assert.assertEquals(expectedMillis, timeDuration.getMillis());
```

#### ✅ Parser Tests

```java
String[] recipe = new String[] {
    "aggregate-stats :data_transfer_size :response_time sum_size_mb sum_time_sec"
};
```

#### ✅ Directive Tests

```java
Assert.assertEquals(expectedTotalSizeInMB, (Double) results.get(0).getValue("sum_size_mb"), 0.001);
Assert.assertEquals(expectedTotalTimeInSeconds, (Double) results.get(0).getValue("sum_time_sec"), 0.001);
```

---

## 🤖 AI Prompts Used in Development

- "How to extend ANTLR grammar to support new token types?"
- "Java class to parse 10KB and convert to bytes"
- "Wrangler directive to aggregate columns"
- "Maven command to regenerate ANTLR parser"
- "JUnit test for custom token class in Java"

---

## ✅ Final Checklist

- [x] Grammar updated and regenerated via ANTLR
- [x] `ByteSize` and `TimeDuration` token classes created
- [x] API and parser enhancements implemented
- [x] `aggregate-stats` directive extended
- [x] Unit and directive-level tests written
- [x] README updated with all instructions

---

## 📌 Conclusion

This enhancement enables Wrangler to understand and manipulate **Byte Size** and **Time Duration** values directly within recipes. With grammar support, API tokens, parser logic, and directive extensions, users can now seamlessly perform advanced aggregations over byte/time data.

