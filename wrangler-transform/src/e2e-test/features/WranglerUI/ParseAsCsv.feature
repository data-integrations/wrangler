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
Feature:  Wrangler - Run time scenarios for parse csv using UI

  @BQ_SOURCE_CSV_TEST @BQ_SOURCE_TEST @BQ_SINK_TEST @BQ_CONNECTION
  Scenario: To verify User is able to run a pipeline using parse csv directive
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
    Then Expand dropdown column: "body" and apply directive: "Parse" as "CSV" with: "Comma" option
    Then Expand dropdown column: "body_3" and apply directive: "FillNullOrEmptyCells" as "shubh"
    Then Enter directive from CLI "rename body_1 new_id"
    Then Enter directive from CLI "quantize body_4 body_q 1:2=20,3:4=40"
    Then Expand dropdown column: "body_4" and apply directive: "ChangeDataType" as "Integer"
    Then Enter directive from CLI "columns-replace s/^new_//g"
    Then Enter directive from CLI "set-headers :abc"
    Then Enter directive from CLI "change-column-case uppercase"
    Then Enter directive from CLI "cleanse-column-names "
    Then Enter directive from CLI "split-to-rows :id '#'"
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
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_parse_csv"
    Given Open Wrangler connections page
    Then Expand connections of type: "BigQuery"
    Then Open action menu for connection: "bqConnectionName" of type: "BigQuery"
    Then Select action: "Delete" for connection: "bqConnectionName" of type: "BigQuery"
    Then Click plugin property: "Delete" button
    Then Verify connection: "bqConnectionName" of type: "BigQuery" is deleted successfully
