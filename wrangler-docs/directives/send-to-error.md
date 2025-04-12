# Send to Error

The SEND-TO-ERROR directive allows the filtering of records and directs the filtered
records that match a given condition to an error collector. If the error collector is not
connected as the next stage in a pipeline, then the filtered records will be dropped.


## Syntax
```
send-to-error <condition> [[metric-name] [error-message]]
```

The `<condition>` is a EL specifing the condition that governs if the record
should be sent to the error collector. Optionally you can specify the metric
name that should be registered everytime a record is sent to error combined
with optional ability to specify a error message that should be recorded.


## Usage Notes

The most common use of the SEND-TO-ERROR directive is to filter out records that are not
part of clean data. This is a data cleansing directive to remove records that do not
conform to specified rules.

The record is sent to the error collector (if connected) when the condition for the record
evaluates to `true`. If the condition evaluates to `false`, the record is passed on
untouched.

## Example

Assume a record that has these three fields:

* Name
* Age
* DOB

As part of a data cleansing process, check that all the records being ingested follow
these rules:

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
send-to-error Age.isEmpty age_empty 'Age field is empty'
send-to-error Name == null name_null
send-to-error Age < 1 || Age > 130 'Age not in range between 1 - 130'
```
