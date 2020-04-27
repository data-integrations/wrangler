# Date and Time
You can use the date and time functions to perform various operations on dates and times in the Wrangler.
Functions that specify `dates`, `times`, or `timestamps` in the arguments use strings with specific formats:
 - For a `date`, the format is `%yyyy-%mm-%dd` and is of java type `LocalDate`.
 - For a `time`, the format is `%hh:%nn:%ss`. If extended to include microseconds, the format is `%hh:%nn:%ss.x` where 
 `x` gives the number of decimal places seconds is given to. `time` is of type `LocalTime`.
 - For a `timestamp`, the format is the date format followed by the time format. `timestamp` is of type `LocalDateTime`.

Functions that have the days of week in the argument take a string that specifies the day of the week. 
The day is specified as a three-letter abbreviation, or the full name. For example, the strings "mon" and "monday" 
are both valid.

## CurrentDate
Returns the `date` when the function is invoked.

#### Namespace
`datetime`

#### Input
None

#### Output
date (`date`)

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
time (`time`)

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
time (`time`)

#### Example   
Use this function to add the current time to a column.
```
set-column timems datetime:CurrentTimeMS()
```

## CurrentTimestamp
Returns `timestamp` that include `date` and `time` when the function is invoked.  

#### Namespace
`datetime`

#### Input
None

#### Output
timestamp (`timestamp`)

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
timestamp (`timestamp`)

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
timestamp (`timestamp`)

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
timestamp (`timestamp`)

#### Example   
Use this function to add the current timestamp including nanoseconds to a column.
```
set-column timestampnano datetime:CurrentTimestampNano()
```

## DateFromDaysSince
Returns a date object by adding an integer to a baseline date. 
The integer can be negative to return a date that is earlier than the baseline date.  

#### Namespace
`datetime`

#### Input
number (`long`), \[baseline date(`date`)\]

#### Output
date (`date`)

#### Example   
If `offset` contains the integer 18250, and `basedate` contains the date `1958–08–18`, 
then the three following functions are equivalent, and return the date `2008–08–05`

```
set-column result datetime:DateFromDaysSince(18250, '1958-08-18')
set-column result datetime:DateFromDaysSince(offset, basedate)
```

If `a` contains the integer -1, and `b` contains the date `1958–08–18`, 
then the following three functions are equivalent, and return the date `1958–08–17`:

```
set-column result datetime:DateFromDaysSince(-1, '1958-08-18')
set-column result datetime:DateFromDaysSince(offset, basedate)
```

## DateFromComponents
Returns a date from the given years, months, and day of month that is given as three separate values.

#### Namespace
`datetime`

#### Input
years (`int`), month(`int`), day of month(`int`) 

#### Output
date (`date`)

#### Example 
If `year` contains the value `2010`, `month` contains the value `12`, 
and `dayofmonth` contains the value `2`, then the two following functions are equivalent, 
and return the date `2010–12–02`  

```
set-column result datetime:DateFromComponents(2010, 12, 2)
set-column result datetime:DateFromDaysSince(year, month, dayofmonth)
```

## DateFromJulianDay
Returns a date from the given Julian day number.

#### Namespace
`datetime`

#### Input
date(`date`) 

#### Output
date (`date`)

#### Example 
If `year` contains the value `2010`, `month` contains the value `12`, 
and `dayofmonth` contains the value `2`, then the two following functions are equivalent, 
and return the date `2010–12–02`  

```
set-column result datetime:DateFromComponents(2010, 12, 2)
set-column result datetime:DateFromDaysSince(year, month, dayofmonth)
```

## DateOffsetByComponents
Returns the given date, with offsets applied from the given year offset, month offset, and day of month offset, 
given as three separate values. The offset values can each be positive, zero, or negative.

#### Namespace
`datetime`

#### Input
baseline date(`date`), year offset(`int`), month of year offset(`int`), day of month offset(`int`) 

#### Output
date (`date`)

#### Example
If `basedate` contains `2011-08-18` and `yearoffset` contains the value `2`, `monthoffset` contains the value `0`, 
and `dayofmonthoffset` contains the value `0`, then the two following functions are equivalent, and 
return the date `2013–08–18`. 

```
set-column result datetime:DateOffsetByComponents('2011-08-18',2,0,0)
set-column result datetime:DateOffsetByComponents(basedate,yearoffset,monthoffset,dayofmonthoffset)
```
If `basedate` contains `2011-08-18` and `yearoffset` contains the value `-2`, `monthoffset` contains the value `0`, 
and `dayofmonthoffset` contains the value `0`, then the two following functions are equivalent, and 
return the date `2009–08–18`. 
```
set-column result datetime:DateOffsetByComponents('2011-08-18',-2,0,0)
set-column result datetime:DateOffsetByComponents(basedate,yearoffset,monthoffset,dayofmonthoffset)
```

## DaysSinceFromDate
Returns the number of days from the source date to the given date. 

#### Namespace
`datetime`

#### Input
first date(`date`), second date(`date`)  

#### Output
days between source and given date (`long`)

#### Example 
If `sourcedate` contains the date `1958–08–18` and `givendate` contains the date `2008–08–18`, 
then the two following functions are equivalent, and return the `long` value `18263`

```
set-column result datetime:DaysSinceFromDate('2008-08-18','1958-08-18')
set-column result datetime:DaysSinceFromDate(sourcedate,givendate)
```

## DaysInMonth
Returns the number of days in the month in the given date. 

#### Namespace
`datetime`

#### Input
date(`date`)  

#### Output
days in month(`int`)

#### Example 
If `basedate` contains the date `1958–08–18`, then the two following functions are equivalent, 
and return the integer value `31`.

```
set-column result datetime:DaysInMonth('1958-08-18')
set-column result datetime:DaysInMonth(basedate)
```

## DaysInYear
Returns the number of days in the year in the given date. 

#### Namespace
`datetime`

#### Input
date(`date`)  

#### Output
days in year(`int`)

#### Example 
If `basedate` contains the date `2012–08–18`, then the two following functions are equivalent, and 
return the `integer` value `366`.
```
set-column result datetime:DaysInYear('2012-08-18')
set-column result datetime:DaysInYear(basedate)
```
If `basedate` contains the date `2011–08–18`, then the two following functions are equivalent, and 
return the `integer` value `365`.
```
set-column result datetime:DaysInYear('2011-08-18')
set-column result datetime:DaysInYear(basedate)
```

## DateOffsetByDays
Returns the given date offset by the given number of days. The offset value can be positive, zero, or negative. 

#### Namespace
`datetime`

#### Input
base date(`date`), day offset(`int`)  

#### Output
date(`date`)

#### Example 
If `basedate` contains `2011-08-18` and `daysoffset` contains the value `2`, then the two following functions 
are equivalent, and return the date `2011–08–20`.
```
set-column result datetime:DateOffsetByDays('2011-08-18', 2)
set-column result datetime:DateOffsetByDays(basedate, daysoffset)
```
If `basedate` contains `2011-08-18` and `daysoffset` contains the value `-31`, then the two following 
functions are equivalent, and return the date `2011–07–18`.
```
set-column result datetime:DateOffsetByDays('2011-08-18', -31)
set-column result datetime:DateOffsetByDays(basedate, daysoffset)
```

## HoursFromTime
Returns the hours portion of a time. 

#### Namespace
`datetime`

#### Input
time(`time`)  

#### Output
hours(`int`)

#### Example 
If `basetime` contains the time `22:30:00`, then the following two functions are equivalent, 
and return the integer value `22`.
```
set-column result datetime:HoursFromTime('22:30:00')
set-column result datetime:HoursFromTime(basetime)
```

## JulianDayFromDate
Returns a Julian day number from the given date. 

#### Namespace
`datetime`

#### Input
date(`date`)  

#### Output
Julian day(`long`)

#### Example 
If `basedate` contains the date `2008–05–27`, then the two following functions are equivalent, 
and return the value `2454614`.
```
set-column result datetime:JulianDayFromDate('2008-05-27')
set-column result datetime:JulianDayFromDate(basedate)
```

## MilliSecondsFromTime
Returns the millisecond portion of a time. 

#### Namespace
`datetime`

#### Input
time(`time`)  

#### Output
milliseconds(`int`)

#### Example 
If `basetime` contains the time `22:30:00.32`, then the following function returns the value `320`:
```
set-column result datetime:MilliSecondsFromTime('22:30:00.32')
set-column result datetime:MilliSecondsFromTime(basetime)
```

## MicroSecondsFromTime
Returns the microsecond portion of a time. 

#### Namespace
`datetime`

#### Input
time(`time`)  

#### Output
microseconds(`int`)

#### Example 
If `basetime` contains the time `22:30:00.32`, then the following function returns the value `320000`:
```
set-column result datetime:MicroSecondsFromTime('22:30:00.32')
set-column result datetime:MicroSecondsFromTime(basetime)
```

## NanoSecondsFromTime
Returns the nanosecond portion of a time. 

#### Namespace
`datetime`

#### Input
time(`time`)  

#### Output
nano seconds(`int`)

#### Example 
If `basetime` contains the time `22:30:00.32`, then the following function returns the value `320000000`:
```
set-column result datetime:NanoSecondsFromTime('22:30:00.32')
set-column result datetime:NanoSecondsFromTime(basetime)
```

## MidnightSecondsFromTime
Returns the number of seconds from midnight to the given time. 

#### Namespace
`datetime`

#### Input
time(`time`)  

#### Output
seconds(`int`)

#### Example 
If `basetime` contains the time `00:30:52`, then the two following functions are equivalent, 
and return the value `1852`:
```
set-column result datetime:MidnightSecondsFromTime('00:30:52')
set-column result datetime:MidnightSecondsFromTime(basetime)
```

## MinutesFromTime
Returns the minutes portion of a time. 

#### Namespace
`datetime`

#### Input
time(`time`)  

#### Output
minutes(`int`)

#### Example 
If `basetime` contains the time `22:30:52`, then the two following functions are equivalent, 
and return the value `30`:
```
set-column result datetime:MinutesFromTime('22:30:52')
set-column result datetime:MinutesFromTime(basetime)
```

## MonthDayFromDate
Returns the day of the month from the given date. 

#### Namespace
`datetime`

#### Input
date(`date`)  

#### Output
day(`int`)

#### Example 
If `basedate` contains the time `2008-08-18`, then the two following functions are equivalent, 
and return the value `18`:
```
set-column result datetime:MonthDayFromDate('2008-08-18')
set-column result datetime:MonthDayFromDate(basedate)
```

## MonthFromDate
Returns the month number from the given date. 

#### Namespace
`datetime`

#### Input
date(`date`)  

#### Output
month number of year (`int`)

#### Example 
If `basedate` contains the time `2008-08-18`, then the two following functions are equivalent, 
and return the value `8`:
```
set-column result datetime:MonthFromDate('2008-08-18')
set-column result datetime:MonthFromDate(basedate)
```

## NextWeekdayFromDate
Returns the date of the specified day of the week soonest after the source date. The day of the week is specified 
as the full name, for example, thursday, or a three-letter abbreviation, for example, `mon`. 

#### Namespace
`datetime`

#### Input
source date(`date`|`string`), day of the week(`int`|`string`)  

#### Output
date(`date`)

#### Example 
If `sourcedate` contains the time `2008-08-18` and the day of the week that is specified is `Thursday` contained 
in `dayofweek`, then the two following functions are equivalent, and return the value `8`:
```
set-column result datetime:NextWeekdayFromDate('2008-08-18', 'thursday')
set-column result datetime:NextWeekdayFromDate('2008-08-18', 'thu')
set-column result datetime:NextWeekdayFromDate(basedate, 'thursday')
set-column result datetime:NextWeekdayFromDate(basedate, dayofweek)
```

## NthWeekdayFromDate
Returns the date of the specified day of the week offset by the specified number of weeks from the source date. 
The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation, for example, 
thu. The offset values can be positive, negative, or zero.

#### Namespace
`datetime`

#### Input
base date(`date`|`string`), day of the week(`string`), week offset(`int`)  

#### Output
date(`date`)

#### Example 
If `basedate` contains the date `2009-08-18` and `Thursday` is specified with an offset of `1`, then the two following 
functions are equivalent, and return the value `2009–08–20`:
```
set-column result datetime:NthWeekdayFromDate('2009-08-18', 'thursday', 1)
set-column result datetime:NthWeekdayFromDate(basedate, 'thu', 1)
```
The first occurrence of Thursday is returned. In the proceeding example, the Thursday occurs in the same week as the 
date `2009-08-18`. The date `2009-08-18` is a `Tuesday`. If `basedate` contains the date `2009-08-18` and `Thursday` is 
specified with an offset of `-2`, then the two following functions are equivalent, and return the value `2009–08–06`:
```
set-column result datetime:NthWeekdayFromDate('2009-08-18', 'thursday', -2)
set-column result datetime:NthWeekdayFromDate(basedate, 'thu', -2)
```

## PreviousWeekdayFromDate
Returns the date of the specified day of the week that is the most recent day before the source date. 
The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation, 
for example, thu.

#### Namespace
`datetime`

#### Input
source date(`date`|`string`), day of the week(`int`|`string`)  

#### Output
date(`date`)

#### Example 
If `basedate` contains the date `2008-08-18` and `Thursday` is specified, then the two following 
functions are equivalent, and return the value `2008–08–14`:
```
set-column result datetime:PreviousWeekdayFromDate('2008-08-18', 'thursday')
set-column result datetime:PreviousWeekdayFromDate(basedate, 'thu')
```

## SecondsFromTime
Returns the seconds portion of a time.

#### Namespace
`datetime`

#### Input
time(`time`|`string`)  

#### Output
second(`int`)

#### Example 
If `basetime` contains the time `22:30:52`, then the two following functions are equivalent, 
and return the value `52`:
```
set-column result datetime:SecondsFromTime('22:30:52')
set-column result datetime:SecondsFromTime(basetime)
```

## SecondsSinceFromTimestamp
Returns the number of seconds between two time stamp objects.

#### Namespace
`datetime`

#### Input
timestamp(`timestamp`|`string`), timestamp base(`timestamp`|`string`) 

#### Output
second(`int`)

#### Example 
If `timestamp` contains the time stamp `2008–08–18 22:30:52`, and `basetimestamp` contains the timestamp 
`2008–08–19 22:30:52`, then the two following functions are equivalent, and return the value `-86400`:
```
set-column result datetime:SecondsSinceFromTimestamp('2008–08–18 22:30:52', '2008–08–19 22:30:52')
set-column result datetime:SecondsSinceFromTimestamp(timestamp, basetimestamp)
```

## SecondsSinceFromTimestamp
Returns the number of seconds between two time stamp objects.

#### Namespace
`datetime`

#### Input
timestamp(`timestamp`|`string`), timestamp base(`timestamp`|`string`) 

#### Output
second(`int`)

#### Example 
If `timestamp` contains the time stamp `2008–08–18 22:30:52`, and `basetimestamp` contains the timestamp 
`2008–08–19 22:30:52`, then the two following functions are equivalent, and return the value `-86400`:
```
set-column result datetime:SecondsSinceFromTimestamp('2008–08–18 22:30:52', '2008–08–19 22:30:52')
set-column result datetime:SecondsSinceFromTimestamp(timestamp, basetimestamp)
```

## TimeDate
Returns the system time and date as a formatted string.

#### Namespace
`datetime`

#### Input
none 

#### Output
current timestamp(`timestamp`)

#### Example 
If the job was run at 4.21 pm on June 20th 2008, then the following function returns the string `16:21:48 20 Jun 2008`
```
set-column result datetime:TimeDate()
```

## TimeFromComponents
Returns a time from the given hours, minutes, seconds, and nanoseconds, given as four separate values.

#### Namespace
`datetime`

#### Input
hours(`int`),minutes(`int`),seconds(`int`),nano(`int`),

#### Output
time(`time`)

#### Example 
If `hour` contains the value `10`, `minutes` contains the value `12`, `seconds` contains the value `2`, 
and `nanoseconds` contains `0`, then the two following functions are equivalent, and return the time `10:12:02.0`
```
set-column result datetime:TimeFromComponents(10, 12, 2, 0)
set-column result datetime:TimeFromComponents(hour, minutes, seconds, nanoseconds)
```

## TimeFromMidnightSeconds
Returns the time given the number of seconds since midnight.

#### Namespace
`datetime`

#### Input
seconds(`int`|`string`)

#### Output
time(`time`)

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

#### Namespace
`datetime`

#### Input
time(`time`|`string`), hours offset(`int`), minutes offset(`int`), seconds offset(`int`)

#### Output
time(`time`)

#### Example 
If `basetime` contains `14:05:29` and `hoursoffset` contains the value `2`, `minutesoffset` contains the value `0`, 
`secondsoffset` contains the value `20`, then the two following functions are equivalent, and return the time `16:05:49`
```
set-column result datetime:TimeOffsetByComponents('14:05:29', 2, 0, 20)
set-column result datetime:TimeOffsetByComponents('14:05:29', 2, 0, 20)
```

## TimestampFromDateTime
Returns a time stamp from the given date and time.

#### Namespace
`datetime`

#### Input
date(`time`|`string`), time(`time`|`string`)

#### Output
timestamp(`timestamp`)

#### Example 
If `basedate` contains the date `2008–08–18` and `basetime` contains the time `22:30:52`, then the two following 
functions are equivalent, and return the timestamp `2008–08–18 22:30:52`
```
set-column result datetime:TimestampFromDateTime('2008–08–18', '22:30:52')
set-column result datetime:TimestampFromDateTime(basedate, basetime)
```

## TimestampFromSecondsSince
Returns a time stamp that is derived from the number of seconds from the base time stamp object.

#### Namespace
`datetime`

#### Input
seconds(`int`|`string`), base timestamp(`timestamp`|`string`)

#### Output
timestamp(`timestamp`)

#### Example 
If `seconds` contains the value `2563` and `basetimestamp` contains the timestamp `2008–08–18 22:30:52`, 
then the two following functions are equivalent, and return the time stamp `2008–08–18 23:13:35`

```
set-column result datetime:TimestampFromSecondsSince(2563, '2008–08–18 22:30:52')
set-column result datetime:TimestampFromSecondsSince('2563', '2008–08–18 22:30:52')
set-column result datetime:TimestampFromSecondsSince(seconds, basetimestamp)
```

## TimestampFromEpoch
Returns a time stamp from the given UNIX epoch time.

#### Namespace
`datetime`

#### Input
seconds(`int`|`string`), base timestamp(`timestamp`|`string`)

#### Output
timestamp(`timestamp`)

#### Example 
If `seconds` contains the value `2563` and `basetimestamp` contains the timestamp `2008–08–18 22:30:52`, 
then the two following functions are equivalent, and return the time stamp `2008–08–18 23:13:35`

```
set-column result datetime:TimestampFromSecondsSince(2563, '2008–08–18 22:30:52')
set-column result datetime:TimestampFromSecondsSince('2563', '2008–08–18 22:30:52')
set-column result datetime:TimestampFromSecondsSince(seconds, basetimestamp)
```

## TimestampFromTime
Returns a timestamp from the given time and timestamp objects. The value in the time object 
overwrites the time value in the time stamp object so that only the date part is used from the time stamp.

#### Namespace
`datetime`

#### Input
time(`time`|`string`), timestamp(`timestamp`|`string`)

#### Output
timestamp(`timestamp`)

#### Example 
If `basetime` contains the time `12:03:22` and `basetimestamp` contains the timestamp `2008–08–18 22:30:52`, 
then the two following functions are equivalent, and return the time stamp `2008–08–18 12:03:22`
```
set-column result datetime:TimestampFromTime('12:03:22', '2008–08–18 22:30:52')
set-column result datetime:TimestampFromTime(basetime, basetimestamp)
```

## TimestampOffsetByComponents
Returns the time stamp, with offsets applied from the base time stamp with year offset, month offset, 
day offset, hour offset, minute offset, and second offset, each given as separate values. 

#### Namespace
`datetime`

#### Input
base timestamp(`timestamp`|`string`), year offset(`int`|`long`), month offset(`int`|`long`), day offset(`int`|`long`), 
hour offset(`int`|`long`),minute offset(`int`|`long`), seconds offset(`int`|`long`) 

#### Output
timestamp(`timestamp`)

#### Example 
If `basetimestamp` contains `2009-08-18 14:05:29` and `yearoffset` contains `0`, `monthsoffset` contains the value `2`, 
`daysoffset` contains the value `-4`, `hoursoffset` contains the value `2`, `minutessoffset` contains the value `0`, 
`secondsoffset` contains the value `20`, then the two following functions are equivalent, and return the 
timestamp `2009-10-14 16:05:49`.
```
set-column result datetime:TimestampOffsetByComponents('2009-08-18 14:05:29', 0, 2, -4, 2, 0, 20)
set-column result datetime:TimestampOffsetByComponents(basetimestamp, yearoffset, monthoffset, dayoffset, 
                                                         hoursoffset, minutesoffset, secondsoffset)
```

## TimestampOffsetBySeconds
Returns the timestamp, with offsets applied from the base timestamp with seconds offset. 

#### Namespace
`datetime`

#### Input
base timestamp(`timestamp`|`string`), seconds offset(`long`|`string`) 

#### Output
timestamp(`timestamp`)

#### Example 
If `basetimestamp` contains `2009-08-18 14:05:29` and `secondsoffset` contains the value `32760`, then the two 
following functions are equivalent, and return the timestamp `2009-08-18 23:11:29`
```
set-column result datetime:TimestampOffsetBySeconds('2009-08-18 14:05:29', 32760)
set-column result datetime:TimestampOffsetBySeconds(basetimestamp, secondsoffset)
```

## EpochFromTimestamp
Returns a UNIX Epoch time value from the given timestamp. 

#### Namespace
`datetime`

#### Input
timestamp(`timestamp`|`string`) 

#### Output
epoch time(`long`)

#### Example 
If `basetimestamp` contains the value `2009–02–13 23:31:30`, then the two following functions are equivalent, 
and return the value `1234567890`
```
set-column result datetime:EpochFromTimestamp('2009–02–13 23:31:30')
set-column result datetime:EpochFromTimestamp(basetimestamp)
```

## WeekdayFromDate
Returns the day number of the week from the given date. Base day optionally specifies the day 
that is regarded as the first in the week and is Sunday by default. 

#### Namespace
`datetime`

#### Input
date(`date`|`string`), [Base day(`int`|`string`)] 

#### Output
day(`int`)

#### Example 
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent, and return the value `1`
```
set-column result datetime:WeekdayFromDate('2008-08-18')
set-column result datetime:WeekdayFromDate(basedate)
```
If `basedate` contains the date `2008-08-18`, and `baseday` contains `saturday`, then the two following 
functions are equivalent, and return the value `2`
```
set-column result datetime:WeekdayFromDate('2008-08-18', "saturday")
set-column result datetime:WeekdayFromDate(basedate,baseday)
```

## YeardayFromDate
Returns the day number in the year from the given date.

#### Namespace
`datetime`

#### Input
date(`date`|`string`) 

#### Output
day(`int`)

#### Example 
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent, 
and return the value `231`
```
set-column result datetime:YeardayFromDate('2008-08-18')
set-column result datetime:YeardayFromDate(basedate)
```

## YearFromDate
Returns the year from the given date.

#### Namespace
`datetime`

#### Input
date(`date`|`string`) 

#### Output
day(`int`)

#### Example 
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent, 
and return the value `2008`
```
set-column result datetime:YearFromDate('2008-08-18')
set-column result datetime:YearFromDate(basedate)
```

## YearweekFromDate
Returns the week number in the year from the given date.

#### Namespace
`datetime`

#### Input
date(`date`|`string`) 

#### Output
week(`int`)

#### Example 
If `basedate` contains the date `2008-08-18`, then the two following functions are equivalent, 
and return the value `33`
```
set-column result datetime:YearFromDate('2008-08-18')
set-column result datetime:YearFromDate(basedate)
```
