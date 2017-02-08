# Fuzzy String Match - Distance

TEXT-DISTANCE directive measures the difference between two sequence of characters.

## Syntax

```
 text-directive <method> <column1> <column2> <destination>
```

```method``` specified measures the distance between the strings of ```column1``` and ```column2```
 and stores the resulting distance measure in the ```destination``` column.

Following are different distance measures supported

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
  text-distance cosine tweet1 tweet2 distance
```

