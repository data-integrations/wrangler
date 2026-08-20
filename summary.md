# 📄 Custom Directive Summary – `aggregate-stats`

This summary describes the **custom User Defined Directive (UDD)** named `aggregate-stats`, created as part of the Software Engineer Intern Assignment for **CDAP DataPrep (Wrangler)**.

---

## 🔧 Directive Name: `aggregate-stats`

**Purpose:**  
Aggregates byte sizes and time durations from all rows, and returns a **single row** with the **total size (in MB)** and **total time (in seconds)**.

---

## 🧠 Syntax
```text
aggregate-stats :<size_col> :<time_col> <total_size_col> <total_time_col>

:<size_col> – column containing byte sizes (e.g., 10KB, 5MB)

:<time_col> – column containing time durations (e.g., 100ms, 1.5s)

<total_size_col> – name of the output column for total size in MB

<total_time_col> – name of the output column for total time in seconds

Sample Recipe:
aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec




💡 Key Features
Works row-by-row and stores totals using ExecutorContext

Only the final result is emitted (as a single row)

Gracefully handles mixed units

Includes edge-case unit tests


📂 New Files Added
File	                        Description
AggregateStatsDirective.java	Main directive logic for aggregation
TimeDuration.java	            Token class to parse time durations
AggregateStatsTest.java	        Unit tests for directive
TokenType.java	                 Updated to include TIME_DURATION and BYTE_SIZE
Grammar test	                 Updated to support new directive in GrammarBasedParserTest.java

Final Notes
This UDD extends the transformation capabilities of CDAP Wrangler by allowing row-wise aggregation of data + time, which is commonly needed in performance and network logging analysis.

You can now use this directive inside Wrangler pipelines or recipes just like any other built-in directive.

