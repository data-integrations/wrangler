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

## Create a new connection

This REST endpoint allows one to create a new connection in the connection
store.

| Attribute  | Description / Example |
| ---------- | --------------------- |
| **HTTP Method**  |     POST  |
| **URL**  | ```/connections/create```     |
| **Example** | [http://localhost:11015/v3/namespaces/default/apps/datapre/services/service/methods/connections/create](http://localhost:11015/v3/namespaces/default/apps/datapre/services/service/methods/connections/create) |
| **Query Params** | None |
| **Request Content Type** | JSON, specification below. |
| **Response Content Type** | JSON |
| **Response Codes** | 200 if OK, 500 if there are any issues |

### Request JSON Object

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


## Update entire connection

When a connection is retrieved using other API.

| Attribute  | Description / Example |
| ---------- | --------------------- |
| **HTTP Method**  |     POST  |
| **URL**  | ```/connections/{id}/update```     |
| **Example** | [http://localhost:11015/v3/namespaces/default/apps/datapre/services/service/methods/connections/mysql_database/update](http://localhost:11015/v3/namespaces/default/apps/datapre/services/service/methods/connections/mysql_database/update) |
| **Query Params** | None |
| **Request Content Type** | JSON, specification as defined in create, but should include fields of the properties. |
| **Response Content Type** | JSON |
| **Response Codes** | 200 if OK, 500 if there are any issues |

## Update properties of a connection

Updates a individual property of a connection.

| Attribute  | Description / Example |
| ---------- | --------------------- |
| **HTTP Method**  |     PUT  |
| **URL**  | ```/connections/{id}/properties?key=<key>&value=<value>```     |
| **Example** | [http://localhost:11015/v3/namespaces/default/apps/datapre/services/service/methods/connections/mysql_database/properties?key=hostname&value=localhost](http://localhost:11015/v3/namespaces/default/apps/datapre/services/service/methods/connections/mysql_database/properties?key=hostname&value=localhost) |
| **Query Params** | ```key``` of the property to be updated, ```value``` for the key to be updated. |
| **Request Content Type** | None |
| **Response Content Type** | JSON |
| **Response Codes** | 200 if OK, 500 if there are any issues |
