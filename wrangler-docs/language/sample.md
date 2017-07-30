# Language Specification

```
set-column :abc, :edf;
set-columns;
send-to-error exp:{ window < 10 } ;
send-to-error exp:{ test < 10 } value;
parse-as-csv :body ' ' true;
change-case upper;
write-as-json-object :col1 :col2,:col3;
!change-case upper;
test-number 10;
test-float 12.4;
test-float 12.5 exp:{test < 10};
set-column :abc exp:{ dq:isNumber(abc) };
parse-as-simple-date :col 'yyyy-mm-dd' :col 'test' :col2,:col4:col9 10 exp:{test < 10};
parse-as-fixed-length :col 3,4,5,6;
quantize :foo 1:2=abc,3:4=efg,5:6='Window';
#pragma window
```