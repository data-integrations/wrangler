# Parsing Apache Log

This recipe show you can use the wrangling directives to parse Apache or NGINX logs.

## Version
To paste this receipe AS-IS, you would need

* Wrangler Service Artifact >= 1.1.0

## Sample Data

[Here](sample/apache-combined-logs.log) is the sample data for running these directives through.

## Recipe
```
  // We start by using the parse directive for parsing the log file
  // We know it's a combined log format, so we specify an alias 'combined'
  // to describe the pattern.

  parse-as-log log combined

  // We don't any more need the main log line as we will be working
  // on the parsed fields -- so we will choose only the fields that
  // we want to project and operate on.
  keep ip_connection_client_host,time_stamp_request_receive_time,http_uri_request_referer,http_useragent_request_user_agent,http_path_request_referer_path,http_protocol_request_referer_protocol,time_date_request_receive_time_date,time_time_request_receive_time_time,bytes_response_body_bytes

  // Date and Time are separated, so we merge them with a dash.
  merge time_date_request_receive_time_date time_time_request_receive_time_time request_recieved_datetime -

  // We drop the base fields for the time.
  drop time_time_request_receive_time_time,time_time_request_receive_time_time

```