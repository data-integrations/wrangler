# Copyright © 2023 Cask Data, Inc.
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
Feature:  Wrangler - Run time scenarios

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the copy count and delete directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_copy_drop_count_setcolmn"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_copy_drop_count_setcolmn"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the fill null and send to error directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_Fillempty_sendtoerror"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_Fillempty_sendtoerror"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the Format,concatenate,title case and copy column directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_Concatenate_titlecase"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_Concatenate_titlecase"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the find and replace,copy column and calculate length directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_FindReplace_copy"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_FindReplace_copy"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the Trim Spaces and parse as csv directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_parsecsv_trim"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_parsecsv_trim"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using cleanse column, current date and title case directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_titlecase_cleanse_currentdate"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_titlecase_cleanse_currentdate"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using decode different date and null empty directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_decode_diffdate_nullempty"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_decode_diffdate_nullempty"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using flatten and format as currency directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_flatten_formatascurrency"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_flatten_formatascurrency"


  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using hash and keep column directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_hash_keep"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_hash_keep"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using encode base 32 base 64 and set variable directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_encodebase32_encodebase64-setvariable"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_encodebase32_encodebase64-setvariable"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using parse as length,increment variable and split rows directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_parseaslength_splitrows_incrementvariable"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_parseaslength_splitrows_incrementvariable"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using parse as simple date,masking,generate uuid and cases directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines for the plugin "Directive_parse_simpledate"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "ExpectedDirective_parse_simpledate"
