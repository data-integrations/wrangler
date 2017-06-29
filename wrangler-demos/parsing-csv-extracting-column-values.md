# Parsing CSV Files and Extracting Column Values

This recipe shows using data prep directives to parse a CSV file and then manipulate
individual fields.


## Version

To paste this receipe as-is requires:

* Wrangler Service Artifact >= 1.1.0


## Sample Data

[Movies CSV File](sample/movies.csv) can be used with this recipe.


## Recipe

To use this recipe in the Data Prep UI, import the sample data into a workspace.
If necessary, rename the column the data is in (`<column-name>`) to `log` using:

```
rename <column-name> log
```

You can now follow the remainder of the recipe:

```
  // Parses the file as CSV records:

  parse-as-csv log ,

  // Drop the record that has been parsed:

  drop log

  // Set the column names:

  set columns movieId,title,genres

  // Remove the header row:

  filter-row-if-matched title title

  // Split the 'genres' column that is pipe-delimited into separate columns:

  split-to-columns genres \|

  // Drop the column that was split in the previous directive:

  drop genres

  // Extract the year from the "Movie (year)" format of the 'title' column:

  extract-regex-groups title [^(]+\(([0-9]{4})\).*

  // Rename the column to 'year':

  rename title_1_1 year

  // Set a default value of "n/a" for these columns:

  fill-null-or-empty genres_2 n/a
  fill-null-or-empty genres_3 n/a
  fill-null-or-empty genres_4 n/a
  fill-null-or-empty genres_5 n/a
```
