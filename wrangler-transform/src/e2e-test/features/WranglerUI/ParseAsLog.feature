# Copyright © 2024 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@Wrangler
Feature:  Wrangler - Run time scenarios for Parse Log

  @BQ_SOURCE_LOG_TEST @BQ_SOURCE_TEST @BQ_SINK_TEST @BQ_CONNECTION
  Scenario: To verify User is able to run a pipeline using parse log directive
    Given Open Wrangler connections page
    Then Click plugin property: "addConnection" button
    Then Click plugin property: "bqConnectionRow"
    Then Enter input plugin property: "name" with value: "bqConnectionName"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details in Wrangler connection page if set in environment variables
    Then Click plugin property: "testConnection" button
    Then Verify the test connection is successful
    Then Click plugin property: "connectionCreate" button
    Then Verify the connection with name: "bqConnectionName" is created successfully
    Then Select connection data row with name: "dataset"
    Then Select connection data row with name: "bqSourceTable"
    Then Verify connection datatable is displayed for the data: "bqSourceTable"
    Then Expand dropdown column: "body" and apply directive: "Parse" as "LOG" with: "Common" option
    Then Expand dropdown column: "number_connection_client_logname_last" and apply directive: "DeleteColumn"
    Then Expand dropdown column: "number_connection_client_logname" and apply directive: "DeleteColumn"
    Then Expand dropdown column: "http_querystring_request_firstline_uri_query" and apply directive: "DeleteColumn"
    Then Expand dropdown column: "http_ref_request_firstline_uri_ref" and apply directive: "FillNullOrEmptyCells" as "no value"
    Then Enter directive from CLI "filter-rows-on condition-false ip_connection_client_host =$ '1'"
    Then Enter directive from CLI "filter-rows-on regex-match string_connection_client_user_last ^Tryck$"
    Then Enter directive from CLI "extract-regex-groups :http_firstline_request_firstline GET*"
    Then Enter directive from CLI "split-to-columns :ip_connection_client_host ,"
    Then Enter directive from CLI "catalog-lookup ICD-9 :Body"
    Then Expand dropdown column: "Body_icd_9_description" and apply directive: "FillNullOrEmptyCells" as "Body Post Catalog lookup"
    Then Enter directive from CLI "set-variable Pass string_request_status_last == 200 ? string_request_status_last : Pass"
    Then Expand dropdown column: "time_day_request_receive_time_last_day" and apply directive: "ChangeDataType" as "Integer"
    Then Click Create Pipeline button and choose the type of pipeline as: "Batch pipeline"
    Then Verify plugin: "BigQueryTable" node is displayed on the canvas with a timeout of 120 seconds
    Then Expand Plugin group in the LHS plugins list: "Sink"
    Then Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Connect plugins: "Wrangler" and "BigQuery2" to establish connection
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_parse_log"
    Given Open Wrangler connections page
    Then Expand connections of type: "BigQuery"
    Then Open action menu for connection: "bqConnectionName" of type: "BigQuery"
    Then Select action: "Delete" for connection: "bqConnectionName" of type: "BigQuery"
    Then Click plugin property: "Delete" button
    Then Verify connection: "bqConnectionName" of type: "BigQuery" is deleted successfully
