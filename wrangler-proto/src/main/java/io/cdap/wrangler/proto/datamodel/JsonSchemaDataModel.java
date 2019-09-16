/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.wrangler.proto.datamodel;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * The POJO for a JsonSchema data model document that is instantiated by deserializing a JsonSchema file.
 * The fields in this proto adhere to JsonSchema draft-07 data model document vocabulary.
 */
public class JsonSchemaDataModel {

  // A keyword field that provides an explanation about the purpose of the instance described by this schema.
  private final String description;

  // A custom extension keyword that maintains a list of all top level entities within the schema.
  // When the data model is describing both top level objects (e.g. tables or resources) and
  // nested data type which may be objects, the discriminator provides a mechanism of distinguishing
  // the schemas.
  private final Discriminator discriminator;

  // A custom extension keyword that maintains information about the JsonSchemaDataModel document along with
  // the standardized data model represented by the document.
  private final MetaData metadata;

  // A keyword field that provides a standardized location for schema authors to inline re-usable
  // Json Schemas into a more general schema.
  private final Map<String, Object> definitions;

  public JsonSchemaDataModel(String description, Discriminator discriminator, MetaData metaData,
                             Map<String, Object> definitions) {
    this.description = description;
    this.discriminator = discriminator;
    this.metadata = metaData;
    this.definitions = definitions;
  }

  /**
   * @return the description explaining the purpose of the schema document.
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * @return the custom discriminator extension that lists all top level objects within the document.
   */
  @Nullable
  public Discriminator getDiscriminator() {
    return discriminator;
  }

  /**
   * @return the custom meta data extension that describes information about the document.
   */
  @Nullable
  public MetaData getMetadata() {
    return metadata;
  }

  /**
   * @return the schema definitions
   */
  @Nullable
  public Map<String, Object> getDefinitions() {
    return definitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JsonSchemaDataModel def = (JsonSchemaDataModel) o;
    return Objects.equals(description, def.description) &&
        Objects.equals(discriminator, def.discriminator) &&
        Objects.equals(metadata, def.metadata) &&
        Objects.equals(definitions, def.definitions);
  }

  /**
   * MetaData maintains information about the JsonSchemaDataModel document and the externally defined
   * standard data model represented by the document. The document version may evolve (e.g. as users
   * make extensions to the data model), while the standard that the evolving data model is derived
   * from remains static. This is a common practice in verticals such as healthcare.
   */
  public class MetaData {

    // The standard data model defined by the JsonSchemaDataModel document.
    private final Standard standard;

    // The version of the JsonSchemaDataModel document.
    private final String version;


    MetaData(Standard standrd, String version) {
      this.standard = standrd;
      this.version = version;
    }

    public Standard getStandard() {
      return standard;
    }

    public String getVersion() {
      return version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MetaData metadata = (MetaData) o;
      return Objects.equals(version, metadata.version) &&
          Objects.equals(standard, metadata.standard);
    }
  }

  /**
   * A standard is an industry specific data model that defines various top level entities,
   * data types, and vocabularies. For example, FHIR and HL7v2 are healthcare specific standards
   * that define a data models widely used in that sector.
   */
  public class Standard {

    private final String name; // The name of the standard (e.g. hl7, fhir, etc).
    private final String version; // The version of the standard (e.g. 2.5.1 for hl7 or r4 for fhir).

    Standard(String name, String version) {
      this.name = name;
      this.version = version;
    }

    public String getName() {
      return name;
    }

    public String getVersion() {
      return version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Standard standard = (Standard) o;
      return Objects.equals(name, standard.name) &&
          Objects.equals(version, standard.version);
    }
  }

  /**
   * Discriminator maintains a list of all top level entities within the schema document.
   * When the data model is describing both top level objects (e.g. tables or resources) and
   * nested data type which may themselves be objects, the discriminator provides a mechanism of
   * distinguishing between these types of schemas within the document.
   */
  public class Discriminator {

    // The property within the source data used to distinguish between top level entity and
    // other data types.
    private final String propertyName;

    // A mapping of top level entity name to the path within the document where the schema is
    // defined.
    private final Map<String, String> mapping;

    Discriminator(String propertyName, Map<String, String> mapping) {
      this.propertyName = propertyName;
      this.mapping = mapping;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public Map<String, String> getMapping() {
      return mapping;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Discriminator desc = (Discriminator) o;
      return Objects.equals(propertyName, desc.propertyName) &&
          Objects.equals(mapping, desc.mapping);
    }
  }
}
