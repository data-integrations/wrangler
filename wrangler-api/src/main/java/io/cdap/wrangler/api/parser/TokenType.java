

 package io.cdap.wrangler.api.parser;

 import io.cdap.wrangler.api.annotations.PublicEvolving;
 
 import java.io.Serializable;
 

 @PublicEvolving
 public enum TokenType implements Serializable {
   
   DIRECTIVE_NAME,
 
  
   COLUMN_NAME,
 
   
   TEXT,
 
  
   NUMERIC,
 
   
   BOOLEAN,
 
   
   COLUMN_NAME_LIST,
 
  
   TEXT_LIST,
 
  
   NUMERIC_LIST,
 
  
   BOOLEAN_LIST,
 
  
   EXPRESSION,
 
   
   PROPERTIES,
 
  
   RANGES,
 
  
   IDENTIFIER,
 
   BYTE_SIZE,
   TIME_DURATION
 }
 
