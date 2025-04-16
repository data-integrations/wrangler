# Date and Time
You can use the date and time functions to perform various operations on dates and times in the Wrangler.
Functions that specify `dates`, `times`, or `datetimes` in the arguments use strings with specific formats:
 - For a `date`, the format is `%yyyy-%mm-%dd` and is of java type `LocalDate`.
 - For a `time`, the format is `%hh:%nn:%ss`. If extended to include microseconds, the format is `%hh:%nn:%ss.x` where
 `x` gives the number of decimal places seconds is given to. `time` is of type `LocalTime`.
 - For a `datetime`, the format is the date format followed by the time format. `datetime` is of type `LocalDateTime`.

Functions that have the days of week in the argument take a string that specifies the day of the week.
The day is specified as a three-letter abbreviation, or the full name. For example, the strings "mon" and "monday"
are both valid.

## GetDate
Returns the `date` represented by the given input.

#### Namespace
`datetime`

#### Input
date(`String`)

#### Output
date (`LocalDate`)

#### Example
Use this function to convert the String to a date.
```
set-column date datetime:GetDate('2008-08-18')
```

## GetTime
Returns the `time` represented by the given input.

#### Namespace
`datetime`

#### Input
time(`String`)

#### Output
time (`LocalTime`)

#### Example
Use this function to convert the String to a time.
```
set-column date datetime:GetTime('22:30:52')
```

## GetDateTime
Returns the `datetime` represented by the given input.

#### Namespace
`datetime`

#### Input
datetime (`String`)

#### Output
datetime (`LocalDateTime`)

#### Example
Use this function to convert the String to a datetime.
```
set-column date datetime:GetDateTime('2008-08-18 22:30:52')
```

## CurrentDate
Returns the `date` when the function is invoked.

#### Namespace
`datetime`

#### Input
None

#### Output
date (`LocalDate`)

#### Example
Use this function to add the current date to a column.
```
set-column date datetime:CurrentDate()
```

## CurrentTime
Returns `time` when the function in invoked.

#### Namespace
`datetime`

#### Input
None

#### Output
time (`LocalTime`)

#### Example
Use this function to add the current time to a column.
```
set-column time datetime:CurrentTime()
```

## CurrentTimeMS
Returns `time` in milliseconds when the function in invoked.

#### Namespace
`datetime`

#### Input
None

#### Output
time (`long`)

#### Example
Use this function to add the current time to a column.
```
set-column timems datetime:CurrentTimeMS()
```

## CurrentDateTime
Returns `datetime` that include `date` and `time` when the function is invoked.

#### Namespace
`datetime`

#### Input
None

#### Output
datetime (`LocalDateTime`)

#### Example
Use this function to add the current datetime to a column.
```
set-column datetime datetime:CurrentDateTime()
```

## CurrentTimestamp
Returns the current `timestamp` in default timezone when the function is invoked.

#### Namespace
`datetime`

#### Input
None

#### Output
timestamp (`ZonedDateTime`)

#### Example
Use this function to add the current timestamp to a column.
```
set-column timestamp datetime:CurrentTimestamp()
```

## CurrentTimestampMS
Returns `timestamp` that include `date` and `time`. The `time` component includes milli seconds.

#### Namespace
`datetime`

#### Input
None

#### Output
timestamp (`long`)

#### Example
Use this function to add the current timestamp including milli seconds to a column.
```
set-column timestampms datetime:CurrentTimestampMS()
```

## CurrentTimestampMicro
Returns `timestamp` that include `date` and `time`. The `time` component includes micro seconds.

#### Namespace
`datetime`

#### Input
None

#### Output
timestamp (`long`)

#### Example
Use this function to add the current timestamp including micro seconds to a column.
```
set-column timestampmicro datetime:CurrentTimestampMicro()
```

## CurrentTimestampNano
Returns `timestamp` that include `date` and `time`. The `time` component includes nano seconds.

#### Namespace
`datetime`

#### Input
None

#### Output
timestamp (`long`)

#### Example
Use this function to add the current timestamp including nanoseconds to a column.
```
set-column timestampnano datetime:CurrentTimestampNano()
```

## DateFromDaysSince
Returns a date object by adding an integer to a baseline date.
The integer can be negative to return a date that is earlier than the baseline date.
If baseline date is not provided, the current date will be used. 
If the number is null, it will be treated as 0. 

#### Namespace
`datetime`

#### Input
number (`Integer`), \[baseline date(`LocalDate`)\]

#### Output
date (`LocalDate`)

#### Example
If `offset` contains the integer 18250, and `basedate` contains the date `1958–08–18`,
then the following functions are equivalent, and return the date `2008–08–05`

```
set-column result datetime:DateFromDaysSince(18250, datetime:GetDate('1958-08-18'))
set-column result datetime:DateFromDaysSince(offset, basedate)
```

If `a` contains the integer -1, and `b` contains the date `1958–08–18`,
then the following functions are equivalent, and return the date `1958–08–17`:

```
set-column result datetime:DateFromDaysSince(-1, datetime:GetDate('1958-08-18'))
set-column result datetime:DateFromDaysSince(a, b)
```

## DateFromComponents
Returns a date from the given years, months, and day of month that is given as three separate values.
If any input is null, null will be returned.

#### Namespace
`datetime`

#### Input
years (`Integer`), month(`Integer`), day of month(`Integer`)

#### Output
date (`LocalDate`)

#### Example
If `year` contains the value `2010`, `month` contains the value `12`,
and `dayofmonth` contains the value `2`, then the two following functions are equivalent,
and return the date `2010–12–02`

```
set-column result datetime:DateFromComponents(2010, 12, 2)
set-column result datetime:DateFromComponents(year, month, dayofmonth)
```

## DateFromJulianDay
Returns a date from the given Julian day number. The day number will be treated as 0 if it is null.

#### Namespace
`datetime`

#### Input
number (`Long`)

#### Output
date (`LocalDate`)

#### Example
If `day` contains the value `2454614`, then the two following functions are equivalent,
and return the date `2008–05–27`

```
set-column result datetime:DateFromJulianDay(2454614)
set-column result datetime:DateFromJulianDay(day)
```

## DateOffsetByComponents
Returns the given date, with offsets applied from the given year offset, month offset, and day of month offset,
given as three separate values. The offset values can each be positive, zero, or negative.
If baseline is null, null will be returned. If the offset is null, it will be treated as 0.

#### Namespace
`datetime`

#### Input
baseline date(`LocalDate`), year offset(`Integer`), month of year offset(`Integer`), day of month offset(`Integer`)

#### Output
date (`LocalDate`)

#### Example
If `basedate` contains `2011-08-18` and `yearoffset` contains the value `2`, `monthoffset` contains the value `0`,
and `dayofmonthoffset` contains the value `0`, then the two following functions are equivalent, and
return the date `2013–08–18`.

```
set-column result datetime:DateOffsetByComponents(datetime:GetDate('2011-08-18'),2,0,0)
set-column result datetime:DateOffsetByComponents(basedate,yearoffset,monthoffset,dayofmonthoffset)
```
If `basedate` contains `2011-08-18` and `yearoffset` contains the value `-2`, `monthoffset` contains the value `0`,
and `dayofmonthoffset` contains the value `0`, then the two following functions are equivalent, and
return the date `2009–08–18`.
```
set-column result datetime:DateOffsetByComponents(datetime:GetDate('2011-08-18'),-2,0,0)
set-column result datetime:DateOffsetByComponents(basedate,yearoffset,monthoffset,dayofmonthoffset)
```

## DaysSinceFromDate
Returns the number of days from the source date to the given date. If any date is null, null will be returned.

#### Namespace
`datetime`

#### Input
first date(`LocalDate`), second date(`LocalDate`)

#### Output
days between source and given date (`Long`)

#### Example
If `sourcedate` contains the date `1958–08–18` and `givendate` contains the date `2008–08–18`,
then the two following functions are equivalent, and return the `long` value `18263`

```
set-column result datetime:DaysSinceFromDate(datetime:GetDate('2008-08-18'),datetime:GetDate('1958-08-18'))
set-column result datetime:DaysSinceFromDate(sourcedate,givendate)
```

## DaysInMonth
Returns the number of days in the month in the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
days in month(`Integer`)

#### Example
If `basedate` contains the date `1958–08–18`, then the two following functions are equivalent,
and return the integer value `31`.

```
set-column result datetime:DaysInMonth(datetime:GetDate('1958-08-18'))
set-column result datetime:DaysInMonth(basedate)
```

## DaysInYear
Returns the number of days in the year in the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
days in year(`Integer`)

#### Example
If `basedate` contains the date `2012–08–18`, then the two following functions are equivalent, and
return the `integer` value `366`.
```
set-column result datetime:DaysInYear(datetime:GetDate('2012-08-18'))
set-column result datetime:DaysInYear(basedate)
```
If `basedate` contains the date `2011–08–18`, then the two following functions are equivalent, and
return the `integer` value `365`.
```
set-column result datetime:DaysInYear(datetime:GetDate('2011-08-18'))
set-column result datetime:DaysInYear(basedate)
```

## DateOffsetByDays
Returns the given date offset by the given number of days. The offset value can be positive, zero, or negative.
If date is null, null will be returned. If offset is null, it will be treated as 0.

#### Namespace
`datetime`

#### Input
base date(`LocalDate`), day offset(`Integer`)

#### Output
date(`LocalDate`)

#### Example
If `basedate` contains `2011-08-18` and `daysoffset` contains the value `2`, then the two following functions
are equivalent, and return the date `2011–08–20`.
```
set-column result datetime:DateOffsetByDays(datetime:GetDate('2011-08-18'), 2)
set-column result datetime:DateOffsetByDays(basedate, daysoffset)
```
If `basedate` contains `2011-08-18` and `daysoffset` contains the value `-31`, then the two following
functions are equivalent, and return the date `2011–07–18`.
```
set-column result datetime:DateOffsetByDays(datetime:GetDate('2011-08-18'), -31)
set-column result datetime:DateOffsetByDays(basedate, daysoffset)
```

## HoursFromTime
Returns the hours portion of a time. If time is null, null will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`)

#### Output
hours(`Integer`)

#### Example
If `basetime` contains the time `22:30:00`, then the following two functions are equivalent,
and return the integer value `22`.
```
set-column result datetime:HoursFromTime(datetime:GetTime('22:30:00'))
set-column result datetime:HoursFromTime(basetime)
```

## JulianDayFromDate
Returns a Julian day number from the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
Julian day(`Long`)

#### Example
If `basedate` contains the date `2008–05–27`, then the two following functions are equivalent,
and return the value `2454614`.
```
set-column result datetime:JulianDayFromDate(datetime:GetDate('2008-05-27'))
set-column result datetime:JulianDayFromDate(basedate)
```

## MilliSecondsFromTime
Returns the millisecond portion of a time. If time is null, null will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`)

#### Output
milliseconds(`Integer`)

#### Example
If `basetime` contains the time `22:30:00.32`, then the following function returns the value `320`:
```
set-column result datetime:MilliSecondsFromTime(datetime:GetTime('22:30:00.32'))
set-column result datetime:MilliSecondsFromTime(basetime)
```

## MicroSecondsFromTime
Returns the microsecond portion of a time. If time is null, null will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`)

#### Output
microseconds(`Integer`)

#### Example
If `basetime` contains the time `22:30:00.32`, then the following function returns the value `320000`:
```
set-column result datetime:MicroSecondsFromTime(datetime:GetTime('22:30:00.32'))
set-column result datetime:MicroSecondsFromTime(basetime)
```

## NanoSecondsFromTime
Returns the nanosecond portion of a time. If time is null, null will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`)

#### Output
nano seconds(`Integer`)

#### Example
If `basetime` contains the time `22:30:00.32`, then the following function returns the value `320000000`:
```
set-column result datetime:NanoSecondsFromTime(datetime:GetTime('22:30:00.32'))
set-column result datetime:NanoSecondsFromTime(basetime)
```

## MidnightSecondsFromTime
Returns the number of seconds from midnight to the given time. If time is null, null will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`)

#### Output
seconds(`Integer`)

#### Example
If `basetime` contains the time `00:30:52`, then the two following functions are equivalent,
and return the value `1852`:
```
set-column result datetime:MidnightSecondsFromTime(datetime:GetTime('00:30:52'))
set-column result datetime:MidnightSecondsFromTime(basetime)
```

## MinutesFromTime
Returns the minutes portion of a time. If time is null, null will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`)

#### Output
minutes(`Integer`)

#### Example
If `basetime` contains the time `22:30:52`, then the two following functions are equivalent,
and return the value `30`:
```
set-column result datetime:MinutesFromTime(datetime:GetTime('22:30:52'))
set-column result datetime:MinutesFromTime(basetime)
```

## MonthDayFromDate
Returns the day of the month from the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
day(`Integer`)

#### Example
If `basedate` contains the time `2008-08-18`, then the two following functions are equivalent,
and return the value `18`:
```
set-column result datetime:MonthDayFromDate(datetime:GetDate('2008-08-18'))
set-column result datetime:MonthDayFromDate(basedate)
```

## MonthFromDate
Returns the month number from the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
month number of year (`Integer`)

#### Example
If `basedate` contains the time `2008-08-18`, then the two following functions are equivalent,
and return the value `8`:
```
set-column result datetime:MonthFromDate(datetime:GetDate('2008-08-18'))
set-column result datetime:MonthFromDate(basedate)
```

## NextWeekdayFromDate
Returns the date of the specified day of the week soonest after the source date. The day of the week is specified
as the full name, for example, thursday, or a three-letter abbreviation, for example, `mon`.
If source date is null, null will be returned. If the day of the week is null, source will be returned.

#### Namespace
`datetime`

#### Input
source date(`LocalDate`), day of the week(`String`)

#### Output
date(`LocalDate`)

#### Example
If `sourcedate` contains the time `2008-08-18` and the day of the week that is specified is `Thursday` contained
in `dayofweek`, then the two following functions are equivalent, and return the value `8`:
```
set-column result datetime:NextWeekdayFromDate(datetime:GetDate('2008-08-18'), 'thursday')
set-column result datetime:NextWeekdayFromDate(datetime:GetDate('2008-08-18'), 'thu')
set-column result datetime:NextWeekdayFromDate(basedate, 'thursday')
set-column result datetime:NextWeekdayFromDate(basedate, dayofweek)
```

## NthWeekdayFromDate
Returns the date of the specified day of the week offset by the specified number of weeks from the source date.
The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation, for example,
thu. The offset values can be positive, negative, or zero.
If source date is null, null will be returned. If day of the week is null, source will be returned.
If offset is null, it will be treated as 0.

#### Namespace
`datetime`

#### Input
source date(`LocalDate`), day of the week(`String`), week offset(`Integer`)

#### Output
date(`LocalDate`)

#### Example
If `basedate` contains the date `2009-08-18` and `Thursday` is specified with an offset of `1`, then the two following
functions are equivalent, and return the value `2009–08–20`:
```
set-column result datetime:NthWeekdayFromDate(datetime:GetDate('2009-08-18'), 'thursday', 1)
set-column result datetime:NthWeekdayFromDate(basedate, 'thu', 1)
```
The first occurrence of Thursday is returned. In the proceeding example, the Thursday occurs in the same week as the
date `2009-08-18`. The date `2009-08-18` is a `Tuesday`. If `basedate` contains the date `2009-08-18` and `Thursday` is
specified with an offset of `-2`, then the two following functions are equivalent, and return the value `2009–08–06`:
```
set-column result datetime:NthWeekdayFromDate(datetime:GetDate('2009-08-18'), 'thursday', -2)
set-column result datetime:NthWeekdayFromDate(basedate, 'thu', -2)
```

## PreviousWeekdayFromDate
Returns the date of the specified day of the week that is the most recent day before the source date.
The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation,
for example, thu.
If source is null, null will be returned. If the day of the week is null, source will be returned.

#### Namespace
`datetime`

#### Input
source date(`LocalDate`), day of the week(`String`)

#### Output
date(`LocalDate`)

#### Example
If `basedate` contains the date `2008-08-18` and `Thursday` is specified, then the two following
functions are equivalent, and return the value `2008–08–14`:
```
set-column result datetime:PreviousWeekdayFromDate(datetime:GetDate('2008-08-18'), 'thursday')
set-column result datetime:PreviousWeekdayFromDate(basedate, 'thu')
```

## SecondsFromTime
Returns the seconds portion of a time. If time is null, null will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`)

#### Output
second(`Integer`)

#### Example
If `basetime` contains the time `22:30:52`, then the two following functions are equivalent,
and return the value `52`:
```
set-column result datetime:SecondsFromTime(datetime:GetTime('22:30:52'))
set-column result datetime:SecondsFromTime(basetime)
```

## SecondsSinceFromDateTime
Returns the number of seconds between two datetime objects. If either datetime is null, null will be returned.

#### Namespace
`datetime`

#### Input
datetime(`LocalDateTime`), datetime base(`LocalDateTime`)

#### Output
second(`Integer`)

#### Example
If `datetime` contains the datetime `2008–08–18 22:30:52`, and `basedatetime` contains the datetime
`2008–08–19 22:30:52`, then the two following functions are equivalent, and return the value `-86400`:
```
set-column result datetime:SecondsSinceFromDateTime(datetime:GetDateTime('2008–08–18 22:30:52'),
                                                    datetime:GetDateTime('2008–08–19 22:30:52'))
set-column result datetime:SecondsSinceFromDateTime(datetime, basedatetime)
```


## TimeDate
Returns the system time and date as a formatted string.

#### Namespace
`datetime`

#### Input
none

#### Output
current datetime(`LocalDateTime`)

#### Example
If the job was run at 4.21 pm on June 20th 2008, then the following function returns the string `16:21:48 20 Jun 2008`
```
set-column result datetime:TimeDate()
```

## TimeFromComponents
Returns a time from the given hours, minutes, seconds, and nanoseconds, given as four separate values.
If any input is null, null will be returned.

#### Namespace
`datetime`

#### Input
hours(`Integer`),minutes(`Integer`),seconds(`Integer`),nano(`Integer`),

#### Output
time(`LocalTime`)

#### Example
If `hour` contains the value `10`, `minutes` contains the value `12`, `seconds` contains the value `2`,
and `microseconds` contains `0`, then the two following functions are equivalent, and return the time `10:12:02.0`
```
set-column result datetime:TimeFromComponents(10, 12, 2, 0)
set-column result datetime:TimeFromComponents(hour, minutes, seconds, microseconds)
```

## TimeFromMidnightSeconds
Returns the time given the number of seconds since midnight. If seconds is null, midnight will be returned.

#### Namespace
`datetime`

#### Input
seconds(`int`)

#### Output
time(`LocalTime`)

#### Example
If `midnightseconds` contains the value `240`, then the two following functions are equivalent, and return the
value `00:04:00`
```
set-column result datetime:TimeFromMidnightSeconds(240)
set-column result datetime:TimeFromMidnightSeconds('240')
set-column result datetime:TimeFromMidnightSeconds(midnightseconds)
```

## TimeOffsetByComponents
Returns the time, with offsets applied from the base time with hour offset, minute offset, and second offset,
each given as separate values.
If time is null, null will be returned. If the offset is null, it will be treated as 0.

#### Namespace
`datetime`

#### Input
time(`LocalTime`), hours offset(`Integer`), minutes offset(`Integer`), seconds offset(`Integer`)

#### Output
time(`LocalTime`)

#### Example
If `basetime` contains `14:05:29` and `hoursoffset` contains the value `2`, `minutesoffset` contains the value `0`,
`secondsoffset` contains the value `20`, then the two following functions are equivalent, and return the time `16:05:49`
```
set-column result datetime:TimeOffsetByComponents(datetime:GetTime('14:05:29'), 2, 0, 20)
set-column result datetime:TimeOffsetByComponents(datetime:GetTime('14:05:29'), 2, 0, 20)
```

## GetDateTime
Returns a datetime from the given date and time. If any input is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`), time(`LocalTime`)

#### Output
datetime(`LocalDateTime`)

#### Example
If `basedate` contains the date `2008–08–18` and `basetime` contains the time `22:30:52`, then the two following
functions are equivalent, and return the datetime `2008–08–18 22:30:52`
```
set-column result datetime:GetDateTime(datetime:GetDate('2008–08–18'), datetime:GetTime('22:30:52'))
set-column result datetime:GetDateTime(basedate, basetime)
```

## DateTimeFromSecondsSince
Returns a datetime that is derived from the number of seconds from the base datetime object.
If datetime is null, null will be returned. If seconds is null, time will be returned.

#### Namespace
`datetime`

#### Input
seconds(`Integer`), base datetime(`LocalDateTime`)

#### Output
datetime(`LocalDateTime`)

#### Example
If `seconds` contains the value `2563` and `basedatetime` contains the datetime `2008–08–18 22:30:52`,
then the two following functions are equivalent, and return the datetime `2008–08–18 23:13:35`

```
set-column result datetime:DateTimeFromSecondsSince(2563, datetime:GetDateTime('2008–08–18 22:30:52'))
set-column result datetime:DateTimeFromSecondsSince(seconds, basedatetime)
```

## DateTimeFromEpoch
Returns a datetime from the given UNIX epoch time specified in seconds. This method assumes the timestamp is in UTC. 
If timestamp is null, null will be returned. 

#### Namespace
`datetime`

#### Input
timestamp(`Long`)

#### Output
datetime(`LocalDateTime`)

#### Example
If `timestamp` contains the value `1234567890`,
then the two following functions are equivalent, and return the datetime `2009-02-13 23:31:30`

```
set-column result datetime:DateTimeFromEpoch(1234567890)
set-column result datetime:DateTimeFromEpoch(timestamp)
```

## DateTimeFromTime
Returns a datetime from the given time and datetime objects. The value in the time object overwrites the 
time value in the datetime object so that only the date part is used from the datetime.
If datetime is null, null will be returned. If time is null, datetime will be returned.

#### Namespace
`datetime`

#### Input
time(`LocalTime`), datetime(`LocalDateTime`)

#### Output
datetime(`LocalDateTime`)

#### Example
If `basetime` contains the time `12:03:22` and `basedatetime` contains the datetime `2008–08–18 22:30:52`,
then the two following functions are equivalent, and return the datetime `2008–08–18 12:03:22`
```
set-column result datetime:DatetimeFromTime(datetime:GetTime('12:03:22'), datetime:GetDateTime('2008–08–18 22:30:52'))
set-column result datetime:DatetimeFromTime(basetime, basedatetime)
```

## DateTimeOffsetByComponents
Returns the datetime, with offsets applied from the base datetime with year offset, month offset,
day offset, hour offset, minute offset, and second offset, each given as separate values.
If datetime is null, null will be returned. If offset is null, it will be treated as 0.

#### Namespace
`datetime`

#### Input
base datetime(`LocalDateTime`), year offset(`Integer`), month offset(`Integer`), day offset(`Integer`),
hour offset(`Integer`),minute offset(`Integer`), seconds offset(`Integer`)

#### Output
datetime(`LocalDateTime`)

#### Example
If `basedatetime` contains `2009-08-18 14:05:29` and `yearoffset` contains `0`, `monthsoffset` contains the value `2`,
`daysoffset` contains the value `-4`, `hoursoffset` contains the value `2`, `minutessoffset` contains the value `0`,
`secondsoffset` contains the value `20`, then the two following functions are equivalent, and return the
datetime `2009-10-14 16:05:49`.
```
set-column result datetime:DateTimeOffsetByComponents(datetime:GetDateTime('2009-08-18 14:05:29'), 0, 2, -4, 2, 0, 20)
set-column result datetime:DateTimeOffsetByComponents(basedatetime, yearoffset, monthoffset, dayoffset,
                                                      hoursoffset, minutesoffset, secondsoffset)
```

## DateTimeOffsetBySeconds
Returns the datetime, with offsets applied from the base datetime with seconds offset.
If datetime is null, null will be returned. If seconds is null, it will be treated as 0.

#### Namespace
`datetime`

#### Input
base datetime(`LocalDateTime`), seconds offset(`Integer`)

#### Output
datetime(`LocalDateTime`)

#### Example
If `basedatetime` contains `2009-08-18 14:05:29` and `secondsoffset` contains the value `32760`, then the two
following functions are equivalent, and return the datetime `2009-08-18 23:11:29`
```
set-column result datetime:DateTimeOffsetBySeconds(datetime:GetDateTime('2009-08-18 14:05:29'), 32760)
set-column result datetime:DateTimeOffsetBySeconds(basedatetime, secondsoffset)
```

## EpochFromDateTime
Returns a UNIX Epoch time value in seconds from the given datetime in UTC time. 
If datetime is null, null will be returned.

#### Namespace
`datetime`

#### Input
datetime(`LocalDateTime`)

#### Output
epoch time(`Long`)

#### Example
If `basedatetime` contains the value `2009–02–13 23:31:30`, then the two following functions are equivalent,
and return the value `1234567890`
```
set-column result datetime:EpochFromDateTime(datetime:GetDateTime('2009–02–13 23:31:30'))
set-column result datetime:EpochFromDateTime(basedatetime)
```

## EpochFromTimestamp
Returns a UNIX Epoch time value in seconds from the given timestamp in UTC time. 
If timestamp is null, null will be returned.

#### Namespace
`datetime`

#### Input
timestamp(`ZonedDateTime`)

#### Output
epoch time(`Long`)

#### Example
If `basetimestamp` contains the value `2009–02–13 23:31:30`, then the two following functions are equivalent,
and return the value `1234567890`
```
set-column result datetime:EpochFromDateTime(2009–02–13 23:31:30[UTC]))
set-column result datetime:EpochFromDateTime(basetimestamp)
```


## WeekdayFromDate
Returns the day number of the week from the given date. Base day optionally specifies the day
that is regarded as the first in the week and is Sunday by default.
If date is null, null will be returned. If startOfWeek is null, the date's day number of the week will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`), [Base day(`String`)]

#### Output
day(`Integer`)

#### Example
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent, and return the value `1`
```
set-column result datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'))
set-column result datetime:WeekdayFromDate(basedate)
```
If `basedate` contains the date `2008-08-18`, and `baseday` contains `saturday`, then the two following
functions are equivalent, and return the value `2`
```
set-column result datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'), "saturday")
set-column result datetime:WeekdayFromDate(basedate,baseday)
```

## YeardayFromDate
Returns the day number in the year from the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
day(`Integer`)

#### Example
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent,
and return the value `231`
```
set-column result datetime:YeardayFromDate(datetime:GetDate('2008-08-18'))
set-column result datetime:YeardayFromDate(basedate)
```

## YearFromDate
Returns the year from the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
day(`Integer`)

#### Example
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent,
and return the value `2008`
```
set-column result datetime:YearFromDate(datetime:GetDate('2008-08-18'))
set-column result datetime:YearFromDate(basedate)
```

## YearweekFromDate
Returns the week number in the year from the given date. If date is null, null will be returned.

#### Namespace
`datetime`

#### Input
date(`LocalDate`)

#### Output
week(`Integer`)

#### Example
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent,
and return the value `33`
```
set-column result datetime:YearweekFromDate(datetime:GetDate('2008-08-18'))
set-column result datetime:YearweekFromDate(basedate)
```