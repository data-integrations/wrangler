# Parsing Apache Log Files

This recipe shows using data prep directives to parse Apache or NGINX logs.


## Version

To paste this receipe as-is requires:

* Wrangler Service Artifact >= 1.1.0


## Sample Data

[Sample Apache Log Data](sample/apache-combined-logs.log) can be used with this recipe.


## Recipe

To use this recipe in the Data Prep UI, import the sample data into a workspace.
If necessary, rename the column the data is in (`<column-name>`) to `log` using:

```
rename <column-name> log
```

You can now follow the remainder of the recipe:

```
  // We start by using the parse directive for parsing the log file. Since we know it is a
  // combined log format, we specify an alias of 'combined' to describe the pattern:

  parse-as-log log combined

  // As we no longer require the main log line -- we will be working only with the parsed
  // fields -- we will specify the fields that we want to project and operate on:

  keep ip_connection_client_host,time_stamp_request_receive_time,http_uri_request_referer,http_useragent_request_user_agent,http_path_request_referer_path,http_protocol_request_referer_protocol,time_date_request_receive_time_date,time_time_request_receive_time_time,bytes_response_body_bytes

  // Date and Time are separated, so merge them together with a hyphen (-):

  merge time_date_request_receive_time_date time_time_request_receive_time_time request_recieved_datetime -

  // Drop the base fields for the time:

  drop time_time_request_receive_time_time,time_time_request_receive_time_time

```
