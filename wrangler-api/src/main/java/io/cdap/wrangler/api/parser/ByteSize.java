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
  * Parses byte size values like "1MB", "512KB", etc., into bytes.
  */
  public class ByteSize implements Token  {
   private final long bytes;
 
   public ByteSize(String value) {
    //  super(value);
     String val = value.trim().toUpperCase();
     if (val.endsWith("KB")) {
       bytes = (long) (Double.parseDouble(val.replace("KB", "")) * 1024);
     } else if (val.endsWith("MB")) {
       bytes = (long) (Double.parseDouble(val.replace("MB", "")) * 1024 * 1024);
     } else if (val.endsWith("GB")) {
       bytes = (long) (Double.parseDouble(val.replace("GB", "")) * 1024 * 1024 * 1024);
     } else if (val.endsWith("B")) {
       bytes = (long) Double.parseDouble(val.replace("B", ""));
     } else {
       bytes = 0;
     }
   }
 
   public long getBytes() {
     return bytes;
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
 