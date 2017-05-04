# Invoke HTTP REST Call

INVOKE-HTTP is a experimental directive to trigger a HTTP POST request
with body being the fields selected.

## Syntax

```
    invoke-http <url> <column>[,<column>*] <header>[,<header>]*
```

```column``` specifies the value to be sent to the service in the post
request.

## Usage Notes

INVOKE-HTTP directive is used to apply some transformation on data using
some of the existing REST services. This directive passes the columns
selected in the post body as JSON Object. The keys in the JSON object
are the column names and the values and type are derived from the object
stored in the column.

Upon processing of request by the service, INVOKE-HTTP directive expects
the data to be written back in a JSON object. The JSON object written
is then added to the record with keys being the column name and the value
being the value returned for the key. The types are all valid JSON types.

Currently, the JSON response has to be simple types. No nested JSONs are
currently supported.

> NOTE: There is a cost associated with making HTTP calls and should not
be used in a environment while processing a lot of data.

When a HTTP Services requires one or more header to be passed, they
can he specified as key-value pair. E.g.

```
  X-Proxy-Server=0.0.0.0,X-Auth-Type=Basic
```

> NOTE: The key and value are separated by an equal-to(=) character and
headers are separated by comma(,)

## Example

Let's take a simple example to illustrate how the INVOKE-HTTP plugin
would work. Let's assume you have a record as follows:
```
    {
        "latitude" : 24.56,
        "longitude" : -65.23,
        "IMEI" : 212332321313,
        "location" : "Mars City"
    }
```

And we have a couple of service ready to go

* A service to locate the zipcode given latitude and longitude and
* A service to get manufacturer name and date given a IMEI number

Following is the way to invoke the service passing in the right info.
```
    invoke-http http://hostname/v3/api/geo-find latitude,longitude
```

Would translate into a ```POST``` call with body as follows:

```
POST v3/api/geo-find
```

and body as
```
    {
        "latitude" : 24.56,
        "longitude" : -65.23
    }
```
Note only the above two fields are sent to the backend as they are the
only fields specified as parameters to the directive.

In case of any failure, the input record is passed to the error collector
so that it can be re-processed later. 
