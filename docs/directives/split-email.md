# Split Email

SPLIT-EMAIL directive splits email id into 'account' and 'domain'.

## Syntax

```
  split-email <column>
```

```column``` is a column containing email addresses.

## Usage Notes

The SPLIT-EMAIL directive will parse the email into its constituents. It extracts
domain and the account name from the email id string. It splits the string on '@'
character. Following is the standard format.

```
  <account>@<domain>
```

Upon splitting the email address, the directive will create two new columns
appending to the original column name.

* <column>.account &
* <column>.domain

to the column name that contains the original email address.
This directive will split at the first '@' character.

If the email address cannot be parsed correctly, the additional columns will be still
generated, but they would be set to 'null' appropriately.

When the email address field in the record is 'null'

```
  {
    "email" : null
  }
```

the directive would generate the following

```
  {
    "email" : null,
    "email.account" : null,
    "email.domain" : null
  }
```

## Examples

Let's assume a record as follows

```
  {
    "name" : "Root, Joltie",
    "email_address" : "root@mars.com",
  }
```

applying the directive

```
  split-email email_address
```

would generate the following record

```
  {
    "name" : "Root, Joltie",
    "email_address" : "root@mars.com",
    "email_address.account" : "root",
    "email_address.domain" : "mars.com"
  }
```

In case of any errors parsing.