# Upcoming Features

  * **Conditional Recipe** -- Conditional Recipe allows sub-recipes to be executed based on the condition.
  ```
  parse-as-fixed-length :body 3;
  rename :body_1 :rectype;
  if (rectype == '020') {
    parse-as-csv :body_2 ',';
    ...
  }
  if (rectype == '010') {
    parse-as-fixed-length :body_2 2,4,9,15,9,9,2,6,10
    ...
  }
  ```
  * **Tokenize Text** -- Tokenizes text into Words, Characters, Sentences, Lines and Paragraphs.
  * **NGram Token Generation** -- Generates N-Gram tokens, where N is configurable.
