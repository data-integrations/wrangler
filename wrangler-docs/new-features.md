# New Features

More [here](wrangler-docs/upcoming-features.md) on upcoming features.

  * **User Defined Directives, also known as UDD**, allow you to create custom functions to transform records within CDAP DataPrep or a.k.a Wrangler. CDAP comes with a comprehensive library of functions. There are however some omissions, and some specific cases for which UDDs are the solution. Additional information on how you can build your custom directives [here](wrangler-docs/custom-directive.md).
    * Migrating directives from version 1.0 to version 2.0 [here](wrangler-docs/directive-migration.md)
    * Information about Grammar [here](wrangler-docs/grammar/grammar-info.md)
    * Various `TokenType` supported by system [here](../api/src/main/java/io/cdap/wrangler/api/parser/TokenType.java)
    * Custom Directive Implementation Internals [here](wrangler-docs/udd-internal.md)

  * A new capability that allows CDAP Administrators to **restrict the directives** that are accessible to their users.
More information on configuring can be found [here](wrangler-docs/exclusion-and-aliasing.md)
