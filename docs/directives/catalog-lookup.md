# Static Catalog Lookup

CATALOG-LOOKUP directive provides lookup into catalogs pre-loaded. Currently it supports looking up
on health care ICD-9 and ICD-10-{2016,2017} codes.

## Syntax

```
 catalog-lookup <catalog> <column>
```
```catalog``` specifies the dictionary into which the ```column``` should be looked up.

Following are catalogs that are currently supported:

* ICD-9
* ICD-10-2016
* ICD-10-2017

## Usage Notes

After the KEEP directive is applied, the column specified in the directive are preserved, but rest all
are removed from the record.


## Example

Let's consider a simple example. Following is the record that contains
one field ```code``` that needs to be looked up.

```
  {
    "code" : "Y36521S",
  }
```

applying following LOOKUP directive with ICD-10 Catalog

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

