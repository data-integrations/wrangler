/*
 * Copyright © 2016-2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by law or agreed to in writing, software
 * distributed under the License is provided on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

 package io.cdap.wrangler.api;

 /**
  * Thrown when an individual Wrangler step encounters a processing failure.
  */
 public class StepException extends Exception {
 
   /**
    * Constructs a StepException with a custom error message.
    *
    * @param message description of the error
    */
   public StepException(String message) {
     super(message);
   }
 
   /**
    * Constructs a StepException with a message and a root cause.
    *
    * @param message description of the error
    * @param cause   the underlying cause of the failure
    */
   public StepException(String message, Throwable cause) {
     super(message, cause);
   }
 }
 
 
 

