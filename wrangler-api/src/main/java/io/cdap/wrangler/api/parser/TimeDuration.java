/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;

/**
  * Parses time duration values like "500ms", "1.5s", etc., into milliseconds.
  */
 public class TimeDuration implements Token {
   private final long milliseconds;
 
   public TimeDuration(String value) {
    //  super(value);
     String val = value.trim().toLowerCase();
     if (val.endsWith("ms")) {
       milliseconds = (long) Double.parseDouble(val.replace("ms", ""));
     } else if (val.endsWith("s")) {
       milliseconds = (long) (Double.parseDouble(val.replace("s", "")) * 1000);
     } else {
       milliseconds = 0;
     }
   }
 
   public long getMilliseconds() {
     return milliseconds;
   }

   @Override
   public Object value() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'value'");
   }

   @Override
   public TokenType type() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'type'");
   }

   @Override
   public JsonElement toJson() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'toJson'");
   }
 }
 