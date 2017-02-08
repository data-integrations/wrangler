# NLP - Stemming Bag of Words

STEMMING directive applies Porter Stemmer for English words. Porter Stemmer is one
 of the best stemmers available in this space. It excellently trades-off between
 speed, readability and accuracy. It stems using a set of rules, or transformations,
 applied in a succession of steps. Generally, it applies around 60 rules in 6 steps.

## Syntax
```
  stemming <column>
```

```column``` contains bag of words either of type string array or string list.

## Usage Notes

The STEMMING directive provides an easy way to apply the stemmer on bag of
 tokenized words. Applying this directive creates an additional column
 ```<column>_porter```.

Depending on the type of the object the field is holding it will be transformed
appropriately.

## Example

Let's consider a very simple example that has tokenized bag of words as string array
or list of strings.
```
{
  "word" : { "how", "are", "you", "doing", "do", "you", "have", "apples" }
}
```

running the directive
```
  stemming words
```

would generate the following record

```
{
  "word" : { "how", "are", "you", "doing", "do", "you", "have", "apples" },
  "word_porter" : { "how", "ar", "you", "do", "do", "you", "have", "appl" }
}
```
