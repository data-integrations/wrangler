# Known Issues

### ⚠️ CLA Not Signed
- This pull request may not be merged into the main Wrangler repository due to a missing Google Contributor License Agreement (CLA).
- For assignment submission purposes, the forked version is complete and functional.

### ⚖️ Floating-Point Precision
- Minor rounding issues may occur in time or size unit conversions (e.g., MB or seconds) due to floating-point precision.
- Assertions in test cases use tolerances to account for this.

### 🧪 Limited Validation
- Input validation for malformed or non-standard units (e.g., `10abc`, `ms1`) is minimal.
- Assumes that input follows standard patterns like `10KB`, `2s`, etc.

### 🔍 Aggregation Scope
- Currently performs **sum**-based aggregation only.
- Optional enhancements like `average`, `p95`, `median`, or custom units are not yet implemented but can be added.

---

For any known limitations above, all features have been manually tested and verified to perform accurately under normal expected inputs.
