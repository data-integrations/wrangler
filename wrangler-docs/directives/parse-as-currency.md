# Parse as CSV

The PARSE-AS-CURRENCY is a directive for parsing a currency value that is a string representation of locale currency
into a number.


## Syntax
```
parse-as-currency <source> <destination> [<locale>]
```

The `<source>` specifies the name of the column that contains string representation of locale currency.
The `<destination>` contains the parsed value of currency as `double`. Optional locale specifies the
locale to be used for parsing the string representation of currency in the `<source>` column.


## Usage Notes

`PARSE-AS-CURRENCY` can be used to parse a string representation of currency into a number. If locale is not
specified in the directive, a default of 'en_US' is assumed.

When the directive is unable to parse the currency, an error record is generated.

Following are few examples of parsing currency into number.

```
parse-as-currency :src :dst
parse-as-currency :src :dst 'en_US'
parse-as-currency :src :dst 'en_IE'
parse-as-currency :src :dst 'pl_PL'
parse-as-currency :src :dst 'ca_ES'
parse-as-currency :src :dst 'es_ES'
parse-as-currency :src :dst 'es_CO'
parse-as-currency :src :dst 'de_CH'
parse-as-currency :src :dst 'en_ZA'
parse-as-currency :src :dst 'en_GB'
parse-as-currency :src :dst 'fr_BE'
```