# Parse as Simple Date

The `parse-as-simple-date` directive parses date strings.

## Syntax

```
parse-as-simple-date <column> <pattern>
```

## Usage Notes

The `parse-as-simple-date` directive will parse a date string, using a pattern string. If
the column is `null` or has already been parsed as a date, applying this directive is a
no-op. The column to be parsed as a date should be of type string.

## Examples

These examples show how date and time patterns are interpreted in the U.S. locale. If the
given date and time is `2001-07-04 12:08:56`, a local time in the U.S. Pacific Time Zone,
then applying different patterns results in these strings:

| Date and Time Pattern          | Date String                          |
| ------------------------------ | ------------------------------------ |
| `yyyy.MM.dd G 'at' HH:mm:ss z` | 2001.07.04 AD at 12:08:56 PDT        |
| `EEE, MMM d, ''yy`             | Wed, Jul 4, '01                      |
| `h:mm a`                       | 12:08 PM                             |
| `hh 'o''clock' a, zzzz`        | 12 o'clock PM, Pacific Daylight Time |
| `K:mm a, z`                    | 0:08 PM, PDT                         |
| `yyyyy.MMMMM.dd GGG hh:mm aaa` | 02001.July.04 AD 12:08 PM            |
| `EEE, d MMM yyyy HH:mm:ss Z`   | Wed, 4 Jul 2001 12:08:56 -0700       |
| `yyMMddHHmmssZ`                | 010704120856-0700                    |
| `yyyy-MM-dd'T'HH:mm:ss.SSSZ`   | 2001-07-04T12:08:56.235-0700         |
| `yyyy-MM-dd'T'HH:mm:ss.SSSXXX` | 2001-07-04T12:08:56.235-07:00        |
| `MM/dd/yyyy HH:mm`             | 07/04/2001 12:09                     |
| `yyyy.MM.dd`                   | 2001-07-04                           |
