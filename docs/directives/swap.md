# Swap Column Names

SWAP is a directive for swapping column names

## Syntax
```
  swap <column1> <colum2>
```

## Usage Notes

The SWAP directive renames the ```column1``` to ```column2``` and ```column2``` to ```column1```.
 If the any of the columns are not present, then the execution of the directive fails.

## Examples

Let's illustrate with a simple example. Following is a simple record
with two fields.

```
{
  "a" : 1,
  "b" : "sample string"
}
```

applying the directive

```
  swap b a
```

Would result in the following record
```
{
  "b" : 1,
  "a" : "sample string"
}
```

