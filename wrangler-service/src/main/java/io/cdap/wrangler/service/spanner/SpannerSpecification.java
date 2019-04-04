/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.spanner;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Spanner specification properties for spanner source plugin
 */
public class SpannerSpecification {
  private final String referenceName;
  // gcp properties
  private final String serviceFilePath;
  private final String project;
  // spanner properties
  private final String instance;
  private final String database;
  private final String table;
  private final String schema;

  SpannerSpecification(String referenceName, String serviceFilePath, String project, String instance,
                       String database, String table, Schema schema) {
    this.referenceName = referenceName;
    this.serviceFilePath = serviceFilePath;
    this.project = project;
    this.instance = instance;
    this.database = database;
    this.table = table;
    this.schema = schema.toString();
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getServiceFilePath() {
    return serviceFilePath;
  }

  public String getProject() {
    return project;
  }

  public String getInstance() {
    return instance;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }
}
