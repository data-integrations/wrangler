# Current Datetime

The CURRENT-DATETIME directive generates the current datetime using the given zone or UTC by default.


## Syntax
```
current-datetime <colname> 'timezone'
```


## Usage Notes

The CURRENT-DATETIME directive generates the current datetime using the given zone or UTC by default.
Zone can be region based string like America/Los_Angeles, Europe/Paris ,
 simple offsets like +08:00 , or prefix and offset like UTC+01:00, GMT+08:00, UT+04:00 etc

## Examples
current-datetime :col1 'UTC-08:00'

current-datetime :col2 'America/Los_Angeles'