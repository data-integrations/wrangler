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

  * `[1]` Specifies the version of directive grammar to be parsed as. 
  * `[3]` Dynamically loads the two UDDs 
  * `[4]` Usage of the directive.


# Container

* Artifact is a container of multiple directives
* Each directive is a plugin using the CDAP Plugin architecture

# Implementation Details

* User issues a directive to load the library and specifies the
  directives he is interested to be loaded.
*
