# Logical Bitwise Functions
You can use the date and time functions to perform various operations on dates and times in the Wrangler.
Functions that specify dates, times, or time stamps in the arguments use strings with specific formats:
 - For a date, the format is %yyyy-%mm-%dd
 - For a time, the format is %hh:%nn:%ss. If extended to include microseconds, the format is %hh:%nn:%ss.x where 
 x gives the number of decimal places seconds is given to.
 - For a time stamp, the format is the date format followed by the time format.

Functions that have the days of week in the argument take a string that specifies the day of the week. 
The day is specified as a three-letter abbreviation, or the full name. For example, the strings "thu" and "thursday" 
are both valid.

## CurrentDate

### Input

### Output

### Example
```
set-column val logical:BitAnd(352, 400)
set-column val logical:BitAnd(a,b)
```

## CurrentTime

### Input

### Output

### Example
```
set-column val logical:BitAnd(352, 400)
set-column val logical:BitAnd(a,b)
```