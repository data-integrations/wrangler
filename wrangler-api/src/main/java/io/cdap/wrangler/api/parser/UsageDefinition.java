/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

 package io.cdap.wrangler.api.parser;

 import io.cdap.wrangler.api.Optional;
 
 import java.io.Serializable;
 import java.util.ArrayList;
 import java.util.List;
 
 /**
  * This class {@link UsageDefinition} provides a way for users to registers the argument for UDDs.
  *
  * {@link UsageDefinition} is a collection of {@link TokenDefinition} and the name of the directive
  * itself. Each token specification has an associated ordinal that can be used to position the argument
  * within the directive.
  */
 public final class UsageDefinition implements Serializable {
   // transient so it doesn't show up when serialized using gson in service endpoint responses
   private final transient int optionalCnt;
   private final String directive;
   private final List<TokenDefinition> tokens;
 
   private UsageDefinition(String directive, int optionalCnt, List<TokenDefinition> tokens) {
     this.directive = directive;
     this.tokens = tokens;
     this.optionalCnt = optionalCnt;
   }
 
   public String getDirectiveName() {
     return directive;
   }
 
   public List<TokenDefinition> getTokens() {
     return tokens;
   }
 
   public int getOptionalTokensCount() {
     return optionalCnt;
   }
 
   @Override
   public String toString() {
     StringBuilder sb = new StringBuilder();
     sb.append(directive).append(" ");
 
     int count = tokens.size();
     for (TokenDefinition token : tokens) {
       if (token.optional()) {
         sb.append(" [");
       }
 
       if (token.label() != null) {
         sb.append(token.label());
       } else {
         if (token.type().equals(TokenType.DIRECTIVE_NAME)) {
           sb.append(token.name());
         } else if (token.type().equals(TokenType.COLUMN_NAME)) {
           sb.append(":").append(token.name());
         } else if (token.type().equals(TokenType.COLUMN_NAME_LIST)) {
           sb.append(":").append(token.name()).append(" [,:").append(token.name()).append("]*");
         } else if (token.type().equals(TokenType.BOOLEAN)) {
           sb.append(token.name()).append(" (true/false)");
         } else if (token.type().equals(TokenType.TEXT)) {
           sb.append("'").append(token.name()).append("'");
         } else if (token.type().equals(TokenType.IDENTIFIER) || token.type().equals(TokenType.NUMERIC)) {
           sb.append(token.name());
         } else if (token.type().equals(TokenType.BOOLEAN_LIST) || token.type().equals(TokenType.NUMERIC_LIST)
           || token.type().equals(TokenType.TEXT_LIST)) {
           sb.append(token.name()).append("[,").append(token.name()).append(" ...]*");
         } else if (token.type().equals(TokenType.EXPRESSION)) {
           sb.append("exp:{<").append(token.name()).append(">}");
         } else if (token.type().equals(TokenType.PROPERTIES)) {
           sb.append("prop:{key:value,[key:value]*");
         } else if (token.type().equals(TokenType.RANGES)) {
           sb.append("start:end=[bool|text|numeric][,start:end=[bool|text|numeric]*");
         }
       }
 
       count--;
 
       if (token.optional()) {
         sb.append("]");
       } else {
         if (count > 0) {
           sb.append(" ");
         }
       }
     }
     return sb.toString();
   }
 
   public static UsageDefinition.Builder builder(String directive) {
     return new UsageDefinition.Builder(directive);
   }
 
   public static final class Builder {
     private final String directive;
     private final List<TokenDefinition> tokens;
     private int currentOrdinal;
     private int optionalCnt;
 
     public Builder(String directive) {
       this.directive = directive;
       this.currentOrdinal = 0;
       this.tokens = new ArrayList<>();
       this.optionalCnt = 0;
     }
 
     public void define(String name, TokenType type) {
       TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, Optional.FALSE);
       currentOrdinal++;
       tokens.add(spec);
     }
 
     public void define(String name, TokenType type, String label) {
       TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, Optional.FALSE);
       currentOrdinal++;
       tokens.add(spec);
     }
 
     public void define(String name, TokenType type, boolean optional) {
       TokenDefinition spec = new TokenDefinition(name, type, null, currentOrdinal, optional);
       optionalCnt = optional ? optionalCnt + 1 : optionalCnt;
       currentOrdinal++;
       tokens.add(spec);
     }
 
     public void define(String name, TokenType type, String label, boolean optional) {
       TokenDefinition spec = new TokenDefinition(name, type, label, currentOrdinal, optional);
       optionalCnt = optional ? optionalCnt + 1 : optionalCnt;
       currentOrdinal++;
       tokens.add(spec);
     }
 
     public UsageDefinition build() {
       return new UsageDefinition(directive, optionalCnt, tokens);
     }
   }
 
   /**
    * This method checks if the given TokenType is accepted by the directive.
    *
    * @param type The TokenType to check.
    * @return true if accepted, false otherwise.
    */
    public boolean accepts(TokenType type) {
      switch (type) {
        case STRING:
        case INTEGER:
        case BOOLEAN:
        case FLOAT:
        case IDENTIFIER:
        case BYTE_SIZE:
        case TIME_DURATION:
          return true;
        default:
          return false;
      }
    }
    
 }
 