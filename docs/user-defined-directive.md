# User Defined Directive (UDD)

User Defined Directive (UDD) allow users to develop, deploy and use
data processing directives within the data preparation tool.

# Syntax

```
    #pragma load-directives   
```

# Container

* Artifact is a container of multiple directives
* Each directive is a plugin using the CDAP Plugin architecture

# Implementation Details

* User issues a directive to load the library and specifies the
  directives he is interested to be loaded.
*
