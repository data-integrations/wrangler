# Split Email

SPLIT-EMAIL directive splits email id into 'account' and 'domain'.

## Syntax

```
  split-email <column>
```

```column``` is a column containing email addresses.

## Usage Notes

The SPLIT-EMAIL directive will parse the email address into its constituents - account and domain.

Upon splitting the email address, the directive will create two new columns
appending to the original column name.

* <column>_account &
* <column>_domain


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
    "email_account" : null,
    "email_domain" : null
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
    "email_address_account" : "root",
    "email_address_domain" : "mars.com"
  }
```

In case of any errors parsing.

## Examples

Let's assume a complex records of email ids

```
[
  { "email" : 'root@example.org'  },
  { "email" : 'joltie.xxx@gmail.com'  },
  { "email" : 'joltie_xxx@hotmail.com'  },
  { "email" : 'joltie."@."root."@".@yahoo.com'  },
  { "email" : 'Joltie, Root <joltie.root@hotmail.com>' },
  { "email" : 'Joltie,Root<joltie.root@hotmail.com>'  },
  { "email" : 'Joltie,Root<joltie.root@hotmail.com'  },
]
```

running the directive would result in the following output records

```
[
  { "email" : 'root@example.org', "email_account" : 'root', "email_domain" : 'cask.co'  },
  { "email" : 'joltie.xxx@gmail.com', "email_account" : 'joltie.xxx', "email_domain" : 'gmail.com' },
  { "email" : 'joltie_xxx@hotmail.com', "email_account" : 'joltie_xxx', "email_domain" : 'hotmail.com'  },
  { "email" : 'joltie."@."root."@".@yahoo.com', "email_account" : 'joltie."@."root."@".', "email_domain" : 'yahoo.com'  },
  { "email" : 'Joltie, Root <joltie.root@hotmail.com>', "email_account" : 'joltie.root', "email_domain" : 'hotmail.com'  },
  { "email" : 'Joltie,Root<joltie.root@hotmail.com>', "email_account" : 'joltie.root', "email_domain" : 'hotmail.com'  },
  { "email" : 'Joltie,Root<joltie.root@hotmail.com', "email_account" : null, "email_domain" : null  },
]
```