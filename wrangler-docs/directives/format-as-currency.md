# Parse as CSV

The FORMAT-AS-CURRENCY is a directive for formatting a number as a currency as specified by the locale.


## Syntax
```
format-as-currency <source> <destination> [<locale>]
```

The `<source>` specifies the name of the column that contains number to be converted to currency as specified by locale.
The `<destination>` contains the formatted value of currency as `string`. Optional locale specifies the
locale to be used for formatting the number as string representation of currency in the `<source>` column.


## Usage Notes

`FORMAT-AS-CURRENCY` can be used to format a number representing currency into a locale currency. If locale is not
specified in the directive, a default of 'en_US' is assumed.

When the directive is unable to parse the currency, an error record is generated.

Following are few examples of parsing currency into number.

```
format-as-currency :src :dst
format-as-currency :src :dst 'en_US'
format-as-currency :src :dst 'en_IE'
format-as-currency :src :dst 'pl_PL'
format-as-currency :src :dst 'ca_ES'
format-as-currency :src :dst 'es_ES'
format-as-currency :src :dst 'es_CO'
format-as-currency :src :dst 'de_CH'
format-as-currency :src :dst 'en_ZA'
format-as-currency :src :dst 'en_GB'
format-as-currency :src :dst 'fr_BE'
```