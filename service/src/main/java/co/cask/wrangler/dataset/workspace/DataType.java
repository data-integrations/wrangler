/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.dataset.workspace;

/**
 * This class {@link DataType} defines types of data that can be stored in a workspace.
 */
public enum DataType {
  // This type represents any binary data - avro, protobuf or even other charset.
  BINARY("application/octet-stream"),

  // This defines the text files.
  TEXT("text/plain"),

  // This defines the XML files.
  XML("application/xml"),

  // This defines the JSON files.
  JSON("application/json"),

  // Special format native to Dataprep, this converts the data into records using the delimiter.
  RECORDS("application/data-prep");


  // Defines the type of data.
  String type;

  DataType(String type) {
    this.type = type;
  }

  /**
   * @return Type of content within workspace.
   */
  public String getType() {
    return type;
  }

  /**
   * Converts the string representation of type into a {@link DataType}.
   *
   * @param text representation of the type.
   * @return an instance of {@link DataType} based on it's string representation, null if not found.
   */
  public static DataType fromString(String text) {
    for (DataType b : DataType.values()) {
      if (b.type.equalsIgnoreCase(text)) {
        return b;
      }
    }
    return null;
  }
}
