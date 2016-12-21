# Wrangler Transform

Insert documentation about your plugin in this file.
The UI will display the contents in the reference section of your plugin,
assuming this file is named following a convention of ``[plugin-name]-transform.md``.
The plugin name is case sensitive.

* set format [csv|json] <delimiter> <skip empty lines>
* set columns <name1, name2, ...>
* rename <source> <destination>
* drop <column-name>
* merge <col1> <col2> <destination-column-name> <delimiter>
* uppercase <col>
* lowercase <col>
* titlecase <col>
* indexsplit <source-column-name> <start> <end> <destination-column-name>
* split <source-column-name> <delimiter> <new-column-1> <new-column-2>
* filter-row-by-regex <column> <regex>
* mask-number <column> <mask-pattern>
* mask-shuffle <column>

For examples of plugin documentation, see the
[Hydrator documentation](https://github.com/caskdata/hydrator-plugins/tree/develop/core-plugins/docs).
