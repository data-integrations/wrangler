# Static Catalog Lookup

CATALOG-LOOKUP directive provides lookup into catalogs that are pre-loaded. Currently the directive supports
looking up on health care ICD-9 and ICD-10-{2016,2017} codes.

## Syntax

```
 catalog-lookup <catalog> <column>
```

```catalog``` specifies the dictionary which should be used for looking up the value in the ```column```

Following are catalogs that are currently supported:

* ICD-9
* ICD-10-2016
* ICD-10-2017
* City (IP Lookup for City)
* Country (IP Lookup for Country)

## Usage Notes

Let's consider a simple example. Following is the record that contains
one field ```code``` that needs to be looked up.

```
  {
    "code" : "Y36521S",
  }
```

applying following LOOKUP directive with ICD-10-2016 Catalog

```
  catalog-lookup ICD-10-2016 code
```

would result in the record that has an additional column ```code_description```
that will contain the result of lookup. In case, there is no matching code 'null' is stored
in the ```code_icd_10_2016_description```

```
  {
    "code" : "Y36521S",
    "code_icd_10_2016_description" : "War operations involving indirect blast effect of nuclear weapon, civilian, sequela"
  }
```

In case the ```code``` is null or empty for a record, then a 'null' value is added to the ```column``` field.