# Request Format Specification

Currently, the Data Prep front-end makes a `GET` call with all the directives in the query
arguments. This causes issues on some browsers as there are limits as to how much data can
be pushed using this approach. This document describes the format that is used for sending
a request to the back-end.


## Specification Version

This document covers version 1.0 of the request specification and details how this should
be handled on the front- and back-ends.


## JSON Request Format
```
  {
    "version": 1.0
    "workspace": {
      "name": string,
      "results": number
    },
    "recipe": {
      "directives": [
        "string",
        "string",
        ...
        "string"
      ],
      "save": boolean,
      "name": string
    },
    "sampling": {
      "method": string,
      "seed": number,
      "limit": number
    }
  }
```

### Version

Specifies the version of this specification (`1.0`). If there are any additions or
deletions to the specification, this version would be updated.


### Workspace

This section of the specification provides information about the workspace that directives
are being applied in and the number of records (the `results`) that the request should
return when the results are computed.

| Field     | Mandatory | Description                                                                          |
| :-------- | :-------: | :----------------------------------------------------------------------------------- |
| `name`    | Yes       | Name of the workspace that Data Prep should operate on                               |
| `results` | Yes       | Number of records that should be returned in response to execution of the directives |


### Recipe

This section of the specification contains all of the directives that are to be applied on
the data in the workspace, with an option to save the directives as a recipe with a name.

| Field        | Mandatory | Description                                                                                            |
| :----------- | :-------: | :----------------------------------------------------------------------------------------------------- |
| `directives` | Yes       | List of directives to be applied on the data                                                           |
| `save`       | No        | If `true`, specifies that the directives should be saved. If so, then `name` should also be specified. |
| `name`       | No        | Name of the recipe. This option is valid only when `save` is set to `true`.                            |


### Sampling

This section of the specification provides information about how the input data is to be
sampled.

| Field    | Mandatory | Description                                                                                 |
| :------- | :-------: | :------------------------------------------------------------------------------------------ |
| `method` | Yes       | Type of sampling to be applied while selecting input data. Currently only supports `first`. |
| `seed`   | No        | The random seed to be used when sampling data                                               |
| `limit`  | Yes       | The number of input records to be read from the source when applying directives             |


## Example

A simple example:

```
  {
    "version": 1.0,
    "workspace": {
      "name": "body",
      "results": 100
    },
    "recipe": {
      "directives": [
        "parse-as-csv body ,",
        "drop body",
        "set-columns a,b,c,d"
      ],
      "save": true,
      "name": "my-recipe"
    },
    "sampling": {
      "method": "first",
      "seed": 1,
      "limit": 1000
    }
  }
```
