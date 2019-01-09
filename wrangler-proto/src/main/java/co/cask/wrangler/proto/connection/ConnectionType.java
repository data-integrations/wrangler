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

package co.cask.wrangler.proto.connection;


/**
 * This class {@link ConnectionType} defines different connections from which the data is extracted.
 *
 * TODO: (CDAP-14619) make casing consistent. User requests currently must have the type as upper case,
 *  then the lower case version is stored in several places and returned in several responses.
 */
public enum ConnectionType {
  UPLOAD("upload"),
  FILE("file"),
  DATABASE("database"),
  TABLE("table"),
  S3("s3"),
  GCS("gcs"),
  BIGQUERY("bigquery"),
  KAFKA("kafka"),
  SPANNER("spanner");

  private String type;

  ConnectionType(String type) {
    this.type = type;
  }

  /**
   * @return String representation of enum.
   */
  public String getType() {
    return type;
  }

}
