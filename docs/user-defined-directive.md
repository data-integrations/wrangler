# User Defined Directive (UDD)

User Defined Directive (UDD) allow users to develop, deploy and use
data processing directives within the data preparation tool.

# Syntax

```
    [1] #pragma version 2.0
    [2] parse-as-csv :body ',' true
    [3] #pragma load-directives text-reverse,text-sanatization
    [4] !text-reverse :text
    [5] !text-sanatization :description
```

More description of the above lines. 

  * `[1]` Specifies the version of directive grammar.
  * `[3]` Dynamically loads the two UDDs as CDAP Plugins. 
  * `[4]` Use the directive. `!` specifies the directive is external.

## Related documentation

  * Information about Grammar [here](grammar/grammar-info.md)

