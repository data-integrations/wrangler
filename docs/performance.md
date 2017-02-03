# Performance Evaluation

## Light Data Transformation DMD

Following are the high level transformations being performed on the data

* Parsing of CSV,
* Drop columns,
* Setting defaults on column,
* Changing case, 
* Masking data and 
* Filtering rows based on expression

## Directives

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

## Experiment

Following are two experiments that were run one with 13M records and other with 80M.

### Experiment #1

* Name of the file : resources/Consumer_Compliants_13M.csv
* Number of records : 13,499,973
* Number of bytes : 4,499,534,313 (~ 4GB)
* Number of columns : 18

### Performance Numbers

```
count          = 13,376,053
mean rate      = 64998.50 records/second
1-minute rate  = 64921.29 records/second
5-minute rate  = 46866.70 records/second
15-minute rate = 36149.86 records/second
```

### Experiment #2
* Total Time : 2:52:14 - 3:13:48 = 1294 seconds (21.5 minutes)
* Number of records : 80,999,838 (80M)
* Number of bytes : 26,997,205,878 (~ 26GB)
* Number of columns : 18

### Performance Numbers
```
count          = 13,376,053
mean rate      = 64998.50 records/second
1-minute rate  = 64921.29 records/second
5-minute rate  = 46866.70 records/second
15-minute rate = 36149.86 records/second
```
