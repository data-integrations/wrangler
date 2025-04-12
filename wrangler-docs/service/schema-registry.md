# Schema Registry

Schema Registry provides a serving layer for all types of metadata.
It provides a RESTful interface for storing and retrieving schemas
(AVRO, Protobuf, etc). It stores a versioned history of all schemas,


## Schema Information

| Field                                                                  | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| ID                                                                     | Id of the schema as provided by the user.                        |
| Name | Display name for the schema. |
| Description | User facing description about the schema |
| Created Date | Time in seconds about when the schema was created. |
| Updated Date | Time in seconds about when the schema was last updated. |
| Version | Auto-incremented version of schema. This version is incremented everytime the schema is updated. |
| Type | Type of the schmea, currently supports AVRO and Protobuf-desc |
| Specification | Byte array of the specification of schema |

## RESTful APIs

| API | Method | Path | Response | Description |
| -------------- | ----------------- | -------------- | -------------- | -------------- |
| Create a schema entry | `PUT` | /schemas | 200 - OK, 500 - Error in backend store | Creates an entry in the schema registry. No schema is registred. |
| Add a schema to schema entry | `POST` | /schemas/{id} | 200 - OK, 500 - Error adding schema to schema registry | Adds a versioned schema to schema registry. POST should use `Content-Type: application/octet-stream` |
| Delete all version of schema | `DELETE` | /schemas/{id} | 200 - OK, 500 - Error deleting schema | Deletes the entire schema entry including all the versions of schema. |
| Delete a sepecific version of schema | `DELETE` | /schemas/{id}/versions/{version} | 200 - OK, 500 - Error deleting a version of schema | Deletes a specific version of schema, if schema is not found then a 404 is returned. |
| GET information about a a version of schema | `GET` | /schemas/{id}/versions/{version} | 200 - OK, 500 - Backend error, 404 - Schema id not found | Information about schema version and schema entry |
| GET information about schema entry | `GET` | /schemas/{id} | 200 - OK, 500 - Error | Information about schema entry |
| List version for schema available | `GET` | /schemas/{id}/versions | 200 - OK, 500 - Error | List the versions of schema. |