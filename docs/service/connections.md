# Connection Service

The Connection Service provides RESTful APIs for managing the lifecycle of
connections. All connection information is stored in the connections
store of the dataset.

These are the lifecycle operations supported by the connection service:

* Create a new connection (**POST**, ```${base}/connections/create```),
* Update an entire connection (**POST**, ```${base}/connections/{id}/update```)
* Update properties of a connection (**PUT**, ```${base}/connections/{id}/properties?key=<key>&value=<value>```)
* Retrieve all properties of a connection (**GET**, ```${base}/connections/{id}/properties```)
* Delete a connection (**DELETE**, ```${base}/connections/{id}```)
* Clone a connection (**GET**, ```${base}/connections/{id}/clone```)
* Retrieve information about all of the connections (**GET**, ```${base}/connections```)
* Retrieve information about a connection (**GET** ```${base}/connections/{id}```)

## Base

This is the base URL for the service:

```
http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods
```

## Request JSON Object for creation and complete update

These are the fields that can be in the request:

* name (mandatory)
* type (mandatory; one of:)
  * DATABASE
  * KAFKA
  * S3
* properties (optional)

Here is an example of a JSON Request for creating a connection:

```
{
    "name": "MySQL Database",
    "type":"DATABASE",
    "description":"MySQL Configuration",
    "properties": {
        "hostaname": "localhost",
        "port": "3306"
    }
}
```

Upon successful creation, the ID of the entry is returned. Here is an
example response when creation is successful:

```
{
    "status": 200,
    "message": "Success",
    "count": 1,
    "values": [
        "mysql_database"
    ]
}
```
