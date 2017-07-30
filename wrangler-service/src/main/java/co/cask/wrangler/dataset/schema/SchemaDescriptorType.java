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

package co.cask.wrangler.dataset.schema;

/**
 * This class {@link SchemaDescriptorType} defines types of Schema supported by the Schema registry
 */
public enum SchemaDescriptorType {
  // Represents an AVRO schema.
  AVRO("avro"),

  // Represents a protobuf descriptor schema.
  PROTOBUF_DESC("protobuf-desc"),

  // Represents schema is of type protobuf-binary which is compiled classes on protobuf.
  PROTOBUF_BINARY("protobuf-binary"),

  // Defines copybook for COBOL EBCDIC data.
  COPYBOOK("copybook");

  // Defines the type of data.
  String type;

  SchemaDescriptorType(String type) {
    this.type = type;
  }

  /**
   * @return Type of schema definition.
   */
  public String getType() {
    return type;
  }

  /**
   * Converts the string representation of type into a {@link SchemaDescriptorType}.
   *
   * @param text representation of the type.
   * @return an instance of {@link SchemaDescriptorType} based on it's string representation, null if not found.
   */
  public static SchemaDescriptorType fromString(String text) {
    for (SchemaDescriptorType b : SchemaDescriptorType.values()) {
      if (b.type.equalsIgnoreCase(text)) {
        return b;
      }
    }
    return null;
  }
}
