# Performance Evaluation

## Setup 

### Medium

#### Data Files

* Name of the file : resources/Consumer_Compliants_13M.csv
* Number of records : 13,499,973
* Number of columns : 18

#### Directives

```
  parse-as-csv demo , true
  drop demo
  drop demo_12
  fill-null-or-empty demo_11 N/A
  uppercase demo_17
  mask-number demo_18 xxx###
  drop demo_6
  drop demo_7
  fill-null-or-empty demo_5 N/A
  uppercase demo_3
  filter-row-if-true demo_9 =~ "CA"
  mask-number demo_10 xxx##
  mask-shuffle demo_4
```

####

```
record-measure
             count = 13376053
         mean rate = 64998.50 events/second
     1-minute rate = 64921.29 events/second
     5-minute rate = 46866.70 events/second
    15-minute rate = 36149.86 events/second
```
