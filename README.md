# CDAP Wrangler

CDAP Wrangler is a powerful library designed to facilitate data transformation and manipulation. This project allows users to work with various data types, perform complex transformations, and apply aggregation functions across columns. This README describes the latest enhancements to the Wrangler library, which add native support for handling byte size and time duration units in recipes.

## Enhancements: Byte Size and Time Duration Units Parsers

### Overview
This enhancement introduces native support for byte size and time duration units within CDAP Wrangler recipes. The changes make it easier to work with columns representing data sizes (like KB, MB) or time intervals (like milliseconds, seconds), allowing for aggregation and unit conversion operations without complex multi-step recipes.

### Key Changes
1. **New Lexer Tokens**:
   - `BYTE_SIZE`: For units like Kilobytes (KB), Megabytes (MB), Gigabytes (GB), etc.
   - `TIME_DURATION`: For units like milliseconds (ms), seconds (s), minutes (m), etc.

2. **Grammar Modifications**: 
   - Updated `Directives.g4` to include rules for parsing byte size and time duration units.
   - Lexer and parser rules have been enhanced to recognize and parse these units.

3. **API Updates**:
   - New Java classes `ByteSize.java` and `TimeDuration.java` are introduced, extending the `Token` class to handle the parsing and canonical conversion of byte sizes and time durations.
   - The API now supports `BYTE_SIZE` and `TIME_DURATION` token types for directive arguments.

4. **Core Parser Updates**:
   - New visit methods have been added to support parsing byte size and time duration tokens.
   - The core logic has been modified to handle these new token types during execution.

5. **New Aggregate Directive**:
   - A new directive called `aggregate-stats` has been added to allow aggregation of byte sizes and time durations across rows, including unit conversions (e.g., converting total bytes to MB or total time to seconds).
   - This directive supports various aggregation types such as total, average, median, p95, and p99.

###Installation and Build
1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-handle/wrangler.git
   cd wrangler
   ```
2. **Build the project using Maveny**:
   ```bash
   mvn clean install
   ```
3. **CAfter building, run tests to verify the correctness of your modifications**:
   ```bash
   mvn test "-DfailIfNoTests=false" "-Dcheckstyle.skip=true" "-Dtest=ByteSizeTest,TimeDurationTest,GrammarBasedParserTest"
   ```

