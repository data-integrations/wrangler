/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

 package io.cdap.wrangler.api;

 import io.cdap.wrangler.api.parser.Token;
 import io.cdap.wrangler.api.parser.TokenType;
 
 import java.util.ArrayList;
 import java.util.Iterator;
 import java.util.List;
 import java.util.stream.Collectors;
 
 /**
  * Represents a group of tokens parsed from the grammar.
  */
 public final class TokenGroup {
   private final SourceInfo info;
   private final List<Token> tokens;
 
   public TokenGroup() {
     this.info = null;
     this.tokens = new ArrayList<>();
   }
 
   public TokenGroup(SourceInfo info) {
     this.info = info;
     this.tokens = new ArrayList<>();
   }
 
   public void add(Token token) {
     tokens.add(token);
   }
 
   public int size() {
     return tokens.size();
   }
 
   public Token get(int i) {
     return tokens.get(i);
   }
 
   public Iterator<Token> iterator() {
     return tokens.iterator();
   }
 
   public SourceInfo getSourceInfo() {
     return info;
   }
 
   /**
    * Filters tokens by the specified TokenType.
    *
    * @param type the TokenType to filter by
    * @return a list of tokens matching the specified type
    */
   public List<Token> getTokensByType(TokenType type) {
     return tokens.stream()
         .filter(token -> token.type() == type)
         .collect(Collectors.toList());
   }
 
   /**
    * Checks if the group contains a token of the specified TokenType.
    *
    * @param type the TokenType to check for
    * @return true if the group contains at least one token of the specified type, false otherwise
    */
   public boolean containsTokenType(TokenType type) {
     return tokens.stream().anyMatch(token -> token.type() == type);
   }
 }
