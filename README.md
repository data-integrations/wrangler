# Wrangler Enhancement: Byte Size and Time Duration Units Parsers

This repository contains my submission for enhancing the CDAP Wrangler library by introducing native parsing support for byte size (e.g., KB, MB) and time duration (e.g., ms, s) units.

## ✅ Overview
The assignment required extending the Wrangler grammar, API, and core functionality to handle these new units, along with the implementation of a new aggregate directive that uses them effectively.

## ✅ Completed Tasks
- 🔧 **Grammar Updates**: Added new lexer rules and modified parser logic to recognize BYTE_SIZE and TIME_DURATION tokens.
- 🧠 **API Enhancements**: Created `ByteSize` and `TimeDuration` classes for canonical unit conversions.
- ⚙️ **Core Integration**: Integrated the new tokens into the parsing and execution flow of Wrangler.
- ➕ **New Directive**: Implemented `aggregate-stats`, allowing aggregation over size and time columns with optional unit conversions.
- 🧪 **Testing**: Added comprehensive test cases covering parsing, unit conversions, edge cases, and directive functionality.

## ✅ Testing & Verification
The implementation has been **thoroughly tested** with multiple scenarios:
- Validated parsing of units like `10KB`, `1.5MB`, `5ms`, `2.1s`, etc.
- Verified correctness of aggregated outputs using `TestingRig` with various recipes and input data.
- Ensured consistent unit conversions and edge case handling.

## 📂 Notes
All changes have been committed to this repository, and the solution builds and runs successfully. If AI tools were used, prompt logs are included as `prompts.txt`.

---

Feel free to reach out if any clarification or demo is needed!
