# Datetime To Timestamp

The DATETIME-TO-TIMESTAMP directive converts a datetime value to timestamp with the given zone


## Syntax
```
datetime-to-timestamp <colname> 'timezone'
```


## Usage Notes

The DATETIME-TO-TIMESTAMP directive converts datetime values to 
timestamp values using the given time zone (UTC by default).
Zone can be region based string like America/Los_Angeles, Europe/Paris ,
 simple offsets like +08:00 , or prefix and offset like UTC+01:00, GMT+08:00, UT+04:00 etc
 
 If the column is `null` applying this directive is a no-op. 

## Examples
datetime-to-timestamp :col1 'UTC-08:00'

datetime-to-timestamp :col2 'America/Los_Angeles'