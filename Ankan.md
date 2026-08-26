## Assignment Completion Summary

✅ Modified Grammar: Directives.g4  
✅ New Token Types: ByteSize, TimeDuration  
✅ Grammar Visitor: RecipeVisitor.java  
✅ Directive Implemented: AggregateStats.java  
✅ Tests Passed: ByteSize, TimeDuration, aggregate-stats  
✅ Build Success: See attached log or screenshot


## Summary of Changes and Reasons

This document outlines the changes made to various files in the project and the reasons behind those changes.

---

### 1. **File: GrammarBasedParserTest.java**
#### Changes:
- Added a new test case testByteSizeParsingInRecipe to validate the parsing of byte size values in recipes.
- Added a new test case testInvalidByteSizeFails to ensure invalid byte size formats throw an exception.

#### Reason:
- To ensure the GrammarBasedParser correctly handles byte size tokens (10MB, 1GB, etc.) and fails gracefully for invalid formats (10XB).

---

### 2. **File: ByteSize.java**
#### Changes:
- Implemented the ByteSize class to parse and convert byte size strings (e.g., 10KB, 1.5MB) into their equivalent byte values.
- Added validation to throw an exception for unsupported formats.

#### Reason:
- To provide a robust mechanism for handling byte size tokens in recipes, ensuring compatibility with the grammar and preventing invalid inputs.

---

### 3. **File: TimeDuration.java**
#### Changes:
- Implemented the TimeDuration class to parse and convert time duration strings (e.g., 500ms, 2.5s) into milliseconds.
- Added validation to throw an exception for unsupported formats.

#### Reason:
- To support time duration tokens in recipes, enabling accurate parsing and validation of time-related values.

---

### 4. **File: Directives.g4**
#### Changes:
- Added new lexer rules for BYTE_SIZE and TIME_DURATION tokens to recognize byte size and time duration formats in recipes.
- Updated the value rule to include BYTE_SIZE and TIME_DURATION.

#### Reason:
- To extend the grammar to support byte size and time duration tokens, enabling their use in recipes.

---

### 5. **File: AggregateStatsDirectiveTest.java**
#### Changes:
- Added test cases to validate the aggregation of byte size and time duration values.
- Included tests for both total and average aggregation scenarios.
- Added a test case to ensure invalid size units throw an exception.

#### Reason:
- To verify the correctness of the AggregateStats directive when handling byte size and time duration values, ensuring accurate aggregation and error handling.

---

### 6. **File: AggregateStats.java**
#### Changes:
- Updated the AggregateStats directive to handle byte size and time duration values using the ByteSize and TimeDuration classes.
- Added support for optional output units (MB, s, etc.) for aggregated results.
- Implemented error handling for unsupported units.

#### Reason:
- To enhance the AggregateStats directive with support for byte size and time duration values, enabling more flexible and accurate data aggregation.

---

### 7. **File: RecipeVisitor.java**
#### Changes:
- Added visitor methods for BYTE_SIZE and TIME_DURATION tokens to handle these new token types during recipe parsing.
- Updated the visitValue method to include BYTE_SIZE and TIME_DURATION.

#### Reason:
- To ensure the RecipeVisitor correctly processes byte size and time duration tokens, enabling their use in recipes.

---

### 8. **File: TimeDurationTest.java**
#### Changes:
- Added test cases to validate the parsing of time duration strings (e.g., 500ms, 2.1s, 3min).
- Included a test case to ensure invalid formats throw an exception.

#### Reason:
- To verify the correctness of the TimeDuration class, ensuring accurate parsing and validation of time duration values.

---

### 9. **File: ByteSizeTest.java**
#### Changes:
- Added test cases to validate the parsing of byte size strings (e.g., 10KB, 1.5MB, 1GB).
- Included a test case to ensure invalid formats throw an exception.

#### Reason:
- To verify the correctness of the ByteSize class, ensuring accurate parsing and validation of byte size values.

---

### 10. **File: TokenType.java**
#### Changes:
- Added new enum values BYTE_SIZE and TIME_DURATION to represent the new token types.

#### Reason:
- To extend the TokenType enum to include byte size and time duration tokens, enabling their use in the grammar and directives.

---

## Conclusion

These changes were made to enhance the functionality of the project by adding support for byte size and time duration tokens in recipes. This ensures accurate parsing, validation, and aggregation of these values, improving the overall robustness and flexibility of the system.