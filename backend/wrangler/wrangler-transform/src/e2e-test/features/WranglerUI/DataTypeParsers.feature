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

@Wrangler_Required
Feature:  Runtime Scenarios for datatype parsers

  @BQ_SOURCE_TS_TEST @BQ_SOURCE_TEST @BQ_SINK_TEST @BQ_CONNECTION
  Scenario: To verify User is able to run a pipeline using parse timestamp directive
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
    Then Expand dropdown column: "update_date" and apply directive: "Parse" as "SIMPLEDATE" with: "yyyy-MM-dd" option
    Then Expand dropdown column: "create_date" and apply directive: "Parse" as "SIMPLEDATE" with: "yyyy-MM-dd" option
    Then Enter directive from CLI "parse-timestamp :time"
    Then Enter directive from CLI "parse-as-currency :price :newprice"
    Then Enter directive from CLI "format-as-currency :newprice :format_price"
    Then Enter directive from CLI "diff-date :create_date :update_date :diff_date"
    Then Enter directive from CLI "timestamp-to-datetime :update_date"
    Then Enter directive from CLI "rename :newprice :id"
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
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_parse_Timestamp"
    Given Open Wrangler connections page
    Then Expand connections of type: "BigQuery"
    Then Open action menu for connection: "bqConnectionName" of type: "BigQuery"
    Then Select action: "Delete" for connection: "bqConnectionName" of type: "BigQuery"
    Then Click plugin property: "Delete" button
    Then Verify connection: "bqConnectionName" of type: "BigQuery" is deleted successfully


  @BQ_SOURCE_DATETIME_TEST @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using parse datetime directive
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
    Then Expand dropdown column: "timestamp" and apply directive: "Parse" with directive type: "DATETIME" and select: "Custom_Format" and enter: "yyyy-MM-dd'T'HH:mm:ssX'['z']'"
    Then Enter directive from CLI "current-datetime :create_date"
    Then Enter directive from CLI "datetime-to-timestamp :timestamp"
    Then Enter directive from CLI "format-datetime :create_date 'y'"
    Then Enter directive from CLI "format-date :timestamp yyyy-mm-dd"
    Then Enter directive from CLI "rename timestamp timecolumn"
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
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_parse_DatetimeNew"
    Given Open Wrangler connections page
    Then Expand connections of type: "BigQuery"
    Then Open action menu for connection: "bqConnectionName" of type: "BigQuery"
    Then Select action: "Delete" for connection: "bqConnectionName" of type: "BigQuery"
    Then Click plugin property: "Delete" button
    Then Verify connection: "bqConnectionName" of type: "BigQuery" is deleted successfully
