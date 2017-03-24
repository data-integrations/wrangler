# Request Format

Currently, the Data Prep frontend makes a 'GET' call with all the directives in the query arguments.
This causes issues on different browsers differently as there are limits to how much data can be pushed
over the wire using this approach. So, in this document we will describe the format that we will be using
for sending requests to the backend.

## Specification Version

This document provides version 1.0 specification of the request and the details of how this should
be handled on the FE and backend.

## JSON Request Format
```
  {
    "version" : "1.0"
    "workspace" : {
      "name" : string,
      "results" : number
    },
    "recipe" : {
      "directives" : [
        "string",
        "string",
        ...
        "string"
      ],
      "save" : boolean,
      "name" : string
    },
    "sampling" : {
      "method" : string,
      "seed" : number,
      "limit" : number
    }
  }
```

### Version
Specifies the version of this specification. If there are any additions or deletions to the specification,
this version would be updated.

### Workspace
This section of the specification provides information about the workspace that directives are being
applied in and also the 'results' in terms of number of records that the request should return when the
results are computed.

| Field | Mandatory | Description |
| :---- | :------: | :----- |
| 'name' | Y | Specifies the name of the workspace the Data prep should operate on |
| 'results' | Y | Specifies the number of the records that should be return in response upon execution of directives |

### Directives
This section of the specification provides the ability to list all the directives that need to be
 applied on the data in the workspace, combined with option to save the directives as recipes by a name.

| Field | Mandatory | Description |
| :---- | :------: | :----- |
| 'directives' | Y | List of directives to be applied on the data |
| 'save' | N | Specifies whether the directives have to be saved. If this option is specified, then 'name' should be specified |
| 'name' | N | Specifies the name of the recipe. This option is valid only when 'save' is set to true |

### Sampling
This section of the specification provides information about how the input data needs to be sampled.

| Field | Mandatory | Description |
| :---- | :------: | :----- |
| 'method' | Y | Specifies the type of sampling to be applied while selecting input data. Currently support FIRST only. |
| 'seed'   | N | This specifies the random seed to be used when sampling data. |
| 'number' | Y | Specifies the number of input records to be read from the source to apply directives |

## Example

Following is one simple example

```
  {
    "version" : "1.0",
    "workspace" : {
      "name" : "body",
      "results" : 100
    },
    "recipe" : {
      "directives" : [
        "parse-as-csv body ,",
        "drop body",
        "set-columns a,b,c,d",
      ],
      "save" : true,
      "name" : "my-recipe"
    },
    "sampling" : {
      "method" : "FIRST",
      "seed" : 1,
      "limit" : 1000
    }
  }
```
