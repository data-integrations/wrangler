# Send Records to Error

SEND-TO-ERROR directive allows you filter records. It redirect the
records that match the condition to the error collector if connected.

## Syntax

```
 send-to-error <condition>
```

```condition``` Is a JEXL expression specifing when to send the record
to the error collector.

## Usage Notes

The most common use of SEND-TO-ERROR directive is to filter out records
that are not part of clean data. This is a data cleansing directive to
remove records that do not conform to the rules specified.

The record is sent to the error collector (if connected) when the
condition for the record evaluates to 'true'. If the condition evaluates
to 'false' then the record is passed untouched.

Let's illustrate how this directive would work with a simple example.
Assume a record that has three field.

* Name,
* DOB and
* Age

As part of data cleansing process, you want to make sure that all the
records that are being ingested have right data. In this case, let's
say you want to make sure

* 'Name' is not empty,
* 'Age' is not empty and less than 0 or greater 100 and
* 'DOB' is a valid date.

This is how the above rules can be applied on the data and if there
are any records that match the condition mentioned above, I would like
to move them to error collector for further investigation.

```
  send-to-error Name == null
  send-to-error Age.isEmpty()
  send-to-error Age < 1 || Age > 100
  send-to-error !date:isDate(DOB)
```