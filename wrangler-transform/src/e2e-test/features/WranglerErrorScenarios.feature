# Copyright © 2025 Cask Data, Inc.
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
Feature:  Wrangler - Verify Wrangler Plugin Error scenarios

  @BQ_SOURCE_CSV_TEST @BQ_SOURCE_TEST @Wrangler_Required
  Scenario: Verify Wrangler Plugin error when user selects Precondition Language as SQL
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the file for importing the pipeline for the plugin "Directive_parse_csv"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQueryTable" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Wrangler"
    Then Select radio button plugin property: "expressionLanguage" with value: "sql"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "directives" is displaying an in-line error message: "errorMessageSqlError"

  @BQ_SOURCE_CSV_TEST @BQ_SOURCE_TEST @BQ_CONNECTION @Wrangler_Required
  Scenario: Verify Wrangler Plugin error when user provides invalid input field Name
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
    Then Click Create Pipeline button and choose the type of pipeline as: "Batch pipeline"
    Then Verify plugin: "BigQueryTable" node is displayed on the canvas with a timeout of 120 seconds
    Then Navigate to the properties page of plugin: "Wrangler"
    Then Replace input plugin property: "field" with value: "invalid"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "field" is displaying an in-line error message: "errorMessageInvalidInputFieldName"
    Given Open Wrangler connections page
    Then Expand connections of type: "BigQuery"
    Then Open action menu for connection: "bqConnectionName" of type: "BigQuery"
    Then Select action: "Delete" for connection: "bqConnectionName" of type: "BigQuery"
    Then Click plugin property: "Delete" button
    Then Verify connection: "bqConnectionName" of type: "BigQuery" is deleted successfully
