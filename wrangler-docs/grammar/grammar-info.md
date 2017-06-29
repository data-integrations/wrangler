# Directive Grammar

This page provides more information on the constructs and grammar of dataprep.

## Recipe

```
#pragma version 1.0
#pragma load-directives test-reverse,test-window
parse-as-csv :body ',' true;
send-to-error exp:{ total < 10};
!test :body prop:{ a='b', c='d', e=f, e=1.0};
```

## AST
![AST](grammar-graph.png)

## Building in IDE
Set the following
  * Path : `${base}/wrangler/core/target/generated-sources/antlr4`
  * Package : `co.cask.wrangler.parser`