# Rename Column

RENAME directive renames the existing column named old to a column named new in the record. 

## Syntax

```
 rename <old> <new>
```

```old``` is the name of the old column that has to be renamed and ```new``` is the name of the column that it needs to be renamed to.

## Usage Notes

RENAME will rename the specified column name by replacing it with a new name specicified. The old column name is not more available in record after this directive has been applied on the record. 

RENAME directive will only rename the column that exists. if the column name does not exist in the record, execution of this directive will fail. 

## Example

Let's say we 
```
"query": "OX49 5NU",
            "result": {
                "postcode": "OX49 5NU",
                "quality": 1,
                "eastings": 464447,
                "northings": 195647,
                "country": "England",
                "nhs_ha": "South Central",
                "longitude": -1.06977254466896,
                "latitude": 51.6559271444373,
                "parliamentary_constituency": "Henley",
                "european_electoral_region": "South East",
                "primary_care_trust": "Oxfordshire",
                "region": "South East",
                "lsoa": "South Oxfordshire 011B",
                "msoa": "South Oxfordshire 011",
                "incode": "5NU",
                "outcode": "OX49",
                "admin_district": "South Oxfordshire",
                "parish": "Brightwell Baldwin",
                "admin_county": "Oxfordshire",
                "admin_ward": "Chalgrove",
                "ccg": "NHS Oxfordshire",
                "nuts": "Oxfordshire",
                "codes": {
                    "admin_district": "E07000179",
                    "admin_county": "E10000025",
                    "admin_ward": "E05009735",
                    "parish": "E04008109",
                    "ccg": "E38000136",
                    "nuts": "UKJ14"
                }
            }
```


