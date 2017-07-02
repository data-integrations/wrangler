# Upcoming Features

  * **Conditional Recipe** -- Conditional Recipe allows sub-recipes to be executed based on the condition.
```
/**
 * Recipe for applying directives based on a type of record.
 * This also shows examples of how macros defines of CDAP can 
 * ignored and/or merged with the recipe. 
 */
 
// Load external directives 
#pragma set-property rectype :record_type;
#pragma load-directives encrypt, sentence-detect, invoke-http-get;

// Parse the input record with first three bytes as rectype
// and rest of them as new body.
parse-as-fixed-length :body 3,197;
rename :body_1 ${rectype};
rename :body_2 :body;

// Apply different set of directives based on the 'rectype'
if((${rectype} == '001')) {
  drop ${rectype};
  parse-as-csv :body ',';
  set-headers :fname,:lname,:address,:city,:ssn,:state,:zip;
  // Data validation functions added by the macros 
  ${macro_rectype_001}
  encrypt :ssn;
  if(zip == '94306') {
    set-column :flagged exp: { 1 };
  }
} else if (${rectype} == '002') {
  drop :rectype;
  parse-as-csv :body ':';
  set-headers :lat,:lon,:device;
  send-to-error exp: { dq:isEmpty(device) };
  invoke-http-get 'http://www.example.org' :lat,:lon;
  drop :lat,:lon;
  rename :lat_lon_location :location;
  // More dq checks
  ${macro_rectype_002}
}
```

  * **Tokenize Text** -- Tokenizes text into Words, Characters, Sentences, Lines and Paragraphs.
  * **NGram Token Generation** -- Generates N-Gram tokens, where N is configurable.
