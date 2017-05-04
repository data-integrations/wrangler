# Mask Numbers
MASK-NUMBER is a directive for masking SSN, Credit Card or any other senstitive numbers.

## Syntax
```
    mask-number <column> <mask>
```
Masks the ```column``` data using the ```mask``` specified.

## Usage Notes

MASK-NUMBER provides substitution masking which in general is used for
masking credit card or SSN numbers. This type of masking is called fixed masking,
where the pattern is applied on the fixed length string.

The mask is specified as follows:

* Use of '#' will include the digit from the position in the input string.
* Use of x/X to make the digit at the position (X is converted to lowercase in the resulting string)
* Any other characters will be passed to output as-is if they occur in mask.

## Example

Let's say we have a record as follows:

```
 {
   "name" : "Joltie",
   "ssn1"  : "000001234",
   "ssn2"  : "000-00-1234",
 }
```

Following masking directives applied on the SSN field in the record
```
 mask-number ssn1 xxx-xx-####
 mask-number ssn2 xxx-xx-####
```

will result in the ssn1 and ssn2 fields being masked as follows:
```
 {
   "name" : "Joltie",
   "ssn1"  : "xxx-xx-1234",
   "ssn2"  : "xxx-xx-1234",
 }
```

Masking can be complex, let's assume a another example:

```
 {
   "number" : "0000012349898"
 }
```

applying the following directive

```
 mask-number number xxx-##-xx-##-XXXX-9
```

will result in the following output record.

```
 {
   "number" : "xxx-00-xx-34-xxxx-9"
 }
```