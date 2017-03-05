# Wrangler Transform

A plugin for performing data transformation based on directives. The directives are generated either by an interactive user interface or manual entered into the plugin.

## Plugin Configuration

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Name of the field** | **N** | '*' | Specifies the name of the input field to be used to wrangler or transform. If the value is '*' the all the fields are available for wrangling.  |
| **Precondition Filter** | **N** | false | Precondition filter that is applied before the record is passed on to wrangling. For e.g. you want to remove the header record. |
| **Transformation Directives** | **Y** | N/A | Specifies a series of wrangling directives that are applied on the input record. |
| **Max Error Events** | **N** | 5 | Maximum number of errors to tolerate before bailing out the processing of pipeline. |

## Usage Notes
