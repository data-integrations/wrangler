## Ritesh Ravi - Wrangler Enhancement Assignment

[![Maven Build](https://maven-badges.herokuapp.com/maven-central/io.cdap.wrangler/wrangler-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.cdap.wrangler/wrangler-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

### Overview
This project contains enhancements to the Wrangler DSL engine by introducing support for parsing:

- Byte Size values (e.g., `10MB`, `500KB`)
- Time Duration values (e.g., `5s`, `3m`, `1.5h`)

### 📌 Objective
To extend the grammar and runtime evaluation in Wrangler by:
- Introducing `BYTE_SIZE` and `TIME_DURATION` token types
- Implementing `ByteSize.java` and `TimeDuration.java`
- Registering tokens in `TokenType.java`
- Enhancing visitor pattern for runtime support
- Creating `AggregateStats` directive as real-world usage
- Writing complete JUnit test cases

---

### 🔧 Enhancements Made

#### 1. Grammar Changes (`Directives.g4`)
```antlr
BYTE_SIZE     : [0-9]+('.'[0-9]+)? WS* BYTE_UNIT;
TIME_DURATION : [0-9]+('.'[0-9]+)? WS* TIME_UNIT;

fragment BYTE_UNIT : ('B'|'KB'|'MB'|'GB'|'TB');
fragment TIME_UNIT : ('ns'|'ms'|'s'|'m'|'h'|'d');

byteSizeArg     : BYTE_SIZE;
timeDurationArg : TIME_DURATION;
```

#### 2. New Java Classes
- `wrangler-api/src/main/java/io/cdap/wrangler/api/parser/ByteSize.java`
- `wrangler-api/src/main/java/io/cdap/wrangler/api/parser/TimeDuration.java`

Each parses values and offers conversion utilities (e.g., `.getBytes()`, `.getMilliseconds()`).

#### 3. Token Registration
Updated `TokenType.java`:
```java
public static final TokenType BYTE_SIZE = new TokenType("BYTE_SIZE");
public static final TokenType TIME_DURATION = new TokenType("TIME_DURATION");
```

#### 4. Visitor Pattern Support
Added methods in `CustomDirectivesVisitor.java`:
```java
public Object visitByteSizeArg(DirectivesParser.ByteSizeArgContext ctx) { return new ByteSize(ctx.getText()); }
public Object visitTimeDurationArg(DirectivesParser.TimeDurationArgContext ctx) { return new TimeDuration(ctx.getText()); }
```

#### 5. New Directive: `AggregateStats`
A new directive to sum up byte sizes and time durations across rows and produce final aggregates in desired units.

---

### 🧪 Test Coverage

#### Unit Tests:
- `ByteSizeTest.java`
- `TimeDurationTest.java`
- `AggregateStatsTest.java`

#### Example:
```java
@Test
public void testMegabytesParsing() {
  ByteSize bs = new ByteSize("5MB");
  Assert.assertEquals(5 * 1024 * 1024, bs.getBytes());
}
```

---

### 📦 Build Status

```bash
mvn clean install -pl wrangler-core -am -Dcheckstyle.skip=true
```

✅ All modules compiled successfully
✅ All test cases passed
✅ No checkstyle violations

---

### 📁 Key Directories Modified
```
wrangler-core/src/main/antlr4/io/cdap/wrangler/parser/Directives.g4
wrangler-api/src/main/java/io/cdap/wrangler/api/parser/ByteSize.java
wrangler-api/src/main/java/io/cdap/wrangler/api/parser/TimeDuration.java
wrangler-core/src/main/java/io/cdap/wrangler/parser/CustomDirectivesVisitor.java
wrangler-core/src/main/java/io/cdap/directives/aggregates/AggregateStats.java
```

---

### 🤖 Prompts.txt (AI Support Used)
1. "How to define BYTE_SIZE token in ANTLR grammar"
2. "Java class to parse '10MB' as bytes"
3. "ANTLR visitor method for custom tokens"
4. "JUnit test case for parsing '5s' to milliseconds"
5. "How to use ExecutorContext in directive aggregation"

---

### 🔗 GitHub Branch
👉 [feat/add-byte-time-parsers](https://github.com/riteshravi1002/wrangler/tree/feat/add-byte-time-parsers)

All code, grammar changes, and test cases have been committed to the branch above.

---

### 👋 Author
**Ritesh Ravi**  
Computer Science Student | Software Engineer Intern Applicant  
GitHub: [@riteshravi1002](https://github.com/riteshravi1002)

---

### License
Apache 2.0 License © Cask Data / CDAP Team

