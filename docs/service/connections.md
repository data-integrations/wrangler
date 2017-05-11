# Connection Service

Connection Service provides REST APIs for managing the lifecycle of the
connections. The connection information is all stored in the connections
store in the dataset.

Following are the lifecycle operations supported by the connection service

* Create a new connection (**POST**, ```${base}/connections/create```),
* Update entire connection (**POST**, ```${base}/connections/{id}/update```)
* Update properties of a connection (**PUT**, ```${base}/connections/{id}/properties?key=<key>&value=<value>```),
* Retrieve all properties of a connection (**GET**, ```${base}/connections/{id}/properties```)
* Delete a connection (**DELETE**, ```${base}/connections/{id}```),
* Clone a connection (**GET**, ```${base}/connections/{id}/clone```),
* Retrieve all the connections (**GET**, ```${base}/connections```) &
* Retrieve information about a connection. (**GET** ```${base}/connections/{id}```)

## Base

Following is the base URL for the service.

```http://localhost:11015/v3/namespaces/default/apps/datapre/services/service/methods```

## Request JSON Object for creation and complete update.

Following are the fields that can be in the request.

* name (Mandatory)
* type (Mandatory)
  * DATABASE
  * KAFKA
  * S3
* properties (Optional)

Following is an example of the sample JSON Request for creating a
connection.

```
{
    "name":"MySQL Database",
    "type":"DATABASE",
    "description":"MySQL Configuration",
    "properties" : {
        "hostaname" : "localhost",
        "port" : "3306",
    }
}
```

Upon successful creation, the Id of the entry is returned. Following is an
example response when creation is successful.

```
{
    "status" : 200,
    "message" : "Success",
    "count" : 1,
    "values" : [
        "mysql_database"
    ]
}
```
