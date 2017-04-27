# Send to Error

The `send-to-error` directive allows the filtering of records and directs the filtered
records that match a given condition to an error collector. If the error collector is not
connected as the next stage in a pipeline, then the filtered records will be dropped.

## Syntax

```
 send-to-error <condition>
```

`condition` is a JEXL expression specifing the condition that governs if the record should
be sent to the error collector.

## Usage Notes

The most common use of the `send-to-error` directive is to filter out records that are not
part of clean data. This is a data cleansing directive to remove records that do not
conform to specified rules.

The record is sent to the error collector (if connected) when the condition for the record
evaluates to `true`. If the condition evaluates to `false`, the record is passed on
untouched.

## Example

Assume a record that has three field:

* Name
* Age
* DOB

As part of a data cleansing process, you want to check that all the records being ingested
follow these rules:

* `Name` is not empty
* `Age` is not empty and not less than 1 or greater 130
* `DOB` is a valid date

These directives will implement these rules; any records that match any of these
conditions will be sent to the error collector for further investigation:

```
  send-to-error Name == null
  send-to-error Age.isEmpty()
  send-to-error Age < 1 || Age > 130
  send-to-error !date:isDate(DOB)
```
