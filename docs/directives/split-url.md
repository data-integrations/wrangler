# Split URL

SPLIT-URL directive splits url id into protocol, authority, host, port, path, filename and query.


## Syntax

```
  split-url <column>
```

```column``` is a column containing the url.

## Usage Notes

The SPLIT-URL directive will parse the url constituents.

Upon splitting the url, the directive will create sever new columns
appending to the original column name.

* column_protocol,
* column_authority,
* column_host,
* column_port,
* column_path,
* column_filename &
* column_query.

If the url cannot be parsed correctly, an exception is throw, but if the url column
does not exist then columns with null value is appended to the record.

When the url field in the record is 'null'

```
  {
    "url" : null
  }
```

the directive would generate the following

```
  {
    "url" : null,
    "url_authority" : null,
    "url_protocol" : null,
    "url_host" : null,
    "url_port" : null,
    "url_path" : null,
    "url_filename" : null,
    "url_query" : null
  }
```

## Examples

Let's assume a record as follows

```
  {
    "url" : "http://example.com:80/docs/books/tutorial/index.html?name=networking#DOWNLOADING"
  }
```

applying the directive

```
  split-url url
```

would generate the following record

```
  {
    "url" : "http://example.com:80/docs/books/tutorial/index.html?name=networking#DOWNLOADING",
    "url_authority" : "example.com:80",
    "url_protocol" : "http",
    "url_host" : "example.com",
    "url_port" : 80,
    "url_path" : "/docs/books/tutorial/index.html",
    "url_filename" : "/docs/books/tutorial/index.html?name=networking",
    "url_query" : "name=networking"
  }
```
