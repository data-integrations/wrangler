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

package io.cdap.wrangler.proto.gcs;

import com.google.gson.annotations.SerializedName;

/**
 * Information about a GCS bucket.
 */
public class GCSBucketInfo {
  private final String name;
  private final String type;
  private final String owner;
  @SerializedName("generated-id")
  private final String generatedId;
  @SerializedName("meta-generation")
  private final long metaGeneration;
  private final long created;
  private final boolean wrangle;
  private final boolean directory;

  public GCSBucketInfo(String name, String type, String owner, long metaGeneration, String generatedId,
                       long created, boolean wrangle) {
    this.name = name;
    this.type = type;
    this.owner = owner;
    this.metaGeneration = metaGeneration;
    this.generatedId = generatedId;
    this.created = created;
    this.wrangle = wrangle;
    // this is constant, but is part of the response so must be a field in this class
    this.directory = true;
  }
}
