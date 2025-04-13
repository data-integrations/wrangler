# Wrangler Enhancement Assignment: Byte Size and Time Duration Handling

## Overview
This enhancement adds support for Byte Size and Time Duration handling within the Wrangler framework. The following modifications were made:
- **ANTLR Grammar**: New lexer tokens and parsing rules for Byte Size and Time Duration.
- **API Enhancements**: New `ByteSize` and `TimeDuration` classes, along with conversion methods.
- **Core Parser Enhancements**: Updated `RecipeVisitor` class to process these new token types.
- **Directive Implementation**: The `aggregate-stats` directive now handles Byte Size and Time Duration, supporting operations like `sum`, `average`, and more.
- **Testing**: Unit tests and parser tests were written to ensure correct parsing and aggregation.

---

## Steps to Complete

### 1. Fork & Setup Wrangler Project

1. **Fork the Wrangler repository**:  
   Fork the [Wrangler GitHub repository](https://github.com/BlackSnow5120).
   
2. **Clone and Build**:
   Clone the repository and build it using Maven:
   ```bash
   git clone https://github.com/BlackSnow5120/wrangler.git
   cd wrangler
   mvn clean install
