# Data Prep Transform

This plugin applies data transformation directives on your data records. The directives
are generated either through an interactive user interface or by manual entry into the
plugin.

## Plugin Configuration

| Configuration     | Required | Default | Description                                                           |
| ----------------- | :------: | :-----: | --------------------------------------------------------------------- |
| Input Field       | No       | `*`     | The name of the input field (or `*` for all fields)                   |
| Precondition      | No       | `false` | A filter to be applied before a record is passed to data prep         |
| Directives        | Yes      | n/a     | The series of data prep directives to be applied on the input records |
| Failure Threshold | No       | `1`     | Maximum number of errors tolerated before exiting pipeline processing |

## Directives

There are numerous directives and variations supported by CDAP, documented at
[http://github.com/hydrator/wrangler](http://github.com/hydrator/wrangler).

## Usage Notes

All input record fields are made available to the data prep directives when `*` is used as
the field to be data prepped. They are in the record in the same order as they appear.

Note that if the transform doesn't operate on all of the input record fields or a field is
not configured as part of the output schema, and you are using the `set columns`
directive, you may see inconsistent behavior. Use the `drop` directive to drop any fields
that are not used in the data prep.

A precondition filter is useful to apply filtering on records before the records are
delivered for data prep. To filter a record, specify a condition that will result in
boolean state of `true`.

For example, to filter out all records that are a header record from a CSV file where the
header record is at the start of the file, you could use this filter:

```
  offset == 0
```

This will filter out records that have an `offset` of zero.

This plugin uses the `emiterror` capability to emit records that fail parsing into a
separate error stream, allowing the aggregation of all errors. However, if the _Failure
Threshold_ is reached, then the pipeline will fail.
