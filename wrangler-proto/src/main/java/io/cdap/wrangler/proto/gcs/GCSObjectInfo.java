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

import javax.annotation.Nullable;

/**
 * Information about a GCS object.
 */
public class GCSObjectInfo {
  private final String name;
  private final String type;
  private final String bucket;
  private final String path;
  private final String blob;
  private final Long generation;
  private final Long created;
  private final Long updated;
  private final Long size;
  private final boolean directory;
  private final Boolean wrangle;

  public GCSObjectInfo(String name, String bucket, String path, String blob, @Nullable Long generation,
                       boolean directory) {
    this(name, bucket, path, blob, generation, directory, null, null, null, null, null);
  }

  public GCSObjectInfo(String name, String bucket, String path, String blob, @Nullable Long generation,
                       boolean directory,
                       @Nullable String type, @Nullable Long created, @Nullable Long updated, @Nullable Long size,
                       @Nullable Boolean wrangle) {
    this.name = name;
    this.type = type;
    this.bucket = bucket;
    this.path = path;
    this.blob = blob;
    this.generation = generation;
    this.created = created;
    this.updated = updated;
    this.size = size;
    this.directory = directory;
    this.wrangle = wrangle;
  }
}
