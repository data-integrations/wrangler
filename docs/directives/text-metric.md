# Fuzzy String Match - Metric

TEXT-METRIC directive provides metric between two sequence of characters.

## Syntax

```
 text-metric <method> <column1> <column2> <destination>
```

```method``` specified provides metric between the strings of ```column1``` and ```column2```
 and stores the resulting metric in the ```destination``` column. The value of metric is always between
 0 and 1.

Following are different metrics supported

* euclidean
* cosine
* block-distance
* identity
* block
* dice
* longest-common-subsequence
* longest-common-substring
* overlap-cofficient
* jaccard
* damerau-levenshtein
* generalized-jaccard
* jaro
* simon-white &
* levenshtein


## Usage Notes



## Example

Let's following is the record

```
  text-metric euclidean tweet1 tweet2 value
```

