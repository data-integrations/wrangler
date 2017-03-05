# Wrangler Transform

This plugin applies data transformation directives on your data to transform the records. The directives are generated either by an interactive user interface or manual entered into the plugin.

## Plugin Configuration

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Name of the field** | **N** | '*' | Specifies the name of the input field to be used to wrangler or transform. If the value is '*' the all the fields are available for wrangling.  |
| **Precondition Filter** | **N** | false | Precondition filter that is applied before the record is passed on to wrangling. For e.g. you want to remove the header record. |
| **Transformation Directives** | **Y** | N/A | Specifies a series of wrangling directives that are applied on the input record. |
| **Max Error Events** | **N** | 5 | Maximum number of errors to tolerate before bailing out the processing of pipeline. |

## Directives 

There are 100s of directives and variations supported by the system, please refer to the documentation here : [http://github.com/hydrator/wrangler](http://github.com/hydrator/wrangler)

## Usage Notes

The input record fields are made available to the wrangling directives when '*' is used as the field to be wrangled and they are in the record in the same order as they appear. 

Note, that if wrangling doesn't operate on all the input record fields or the field is not configured as part of output schema and you are using the SET COLUMNS directive, you might see in-consistent behavior. So, please use the DROP directive to drop the fields that are not used in the wrangler. 

Precondition filter is useful when you want to apply filtering on record before the records are delivered for wrangling. In order to filter the record, please specify condition that will result in boolean state 'true'. E.g. if we want to filter out all records that are header record from CSV and header record is at the start of the file, I would use the following filter:

```
  offset == 0 
```

This will filter out records that has 'offset' as zero. 
