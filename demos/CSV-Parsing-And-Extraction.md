# CSV Parsing and Extracting Column values

This recipe show you can use the wrangling directives to
parse a CSV files and it also show how you can micro-wrangle
individual fields.

## Version
To paste this receipe AS-IS, you would need

* Wrangler Service Artifact >= 1.1.0

## Sample Data

[Here](sample/movies.csv) is the sample data for running these directives through.

## Recipe
```
// Parses the file as CSV record
parse-as-csv log ,

// Drop the record that has been parsed
drop log

// Set the column names
set columns movieId,title,genres

// Remove header row
filter-row-if-matched title title

// Split the column that is pipe delimited into columns
split-to-columns genres \|

// Drop the column that was split above
drop genres

// Extract year from “Movie (year)” format
extract-regex-groups title [^(]+\(([0-9]{4})\).*

// rename the column to year
rename title_1_1 year

// Default values for columns
fill-null-or-empty genres_2 NA
fill-null-or-empty genres_3 NA
fill-null-or-empty genres_4 NA
fill-null-or-empty genres_5 NA
```