/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.wrangler.proto.s3;

import co.cask.wrangler.service.FileTypeDetector;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Nullable;

/**
 * Information about an S3 object.
 */
public class S3ObjectInfo {
  private final String name;
  private final String type;
  private final String path;
  private final String owner;
  @SerializedName("class")
  private final String storageClass;
  private final Long created;
  @SerializedName("last-modified")
  private final Long lastModified;
  private final Long size;
  private final Boolean directory;
  private final Boolean wrangle;

  private S3ObjectInfo(String name, String type, @Nullable String path, @Nullable String owner,
                       @Nullable String storageClass, @Nullable Long created, @Nullable Long lastModified,
                       @Nullable Long size, @Nullable Boolean directory, @Nullable Boolean wrangle) {
    this.name = name;
    this.path = path;
    this.type = type;
    this.owner = owner;
    this.storageClass = storageClass;
    this.created = created;
    this.lastModified = lastModified;
    this.size = size;
    this.directory = directory;
    this.wrangle = wrangle;
  }

  public static S3ObjectInfo ofBucket(Bucket bucket) {
    return builder(bucket.getName(), "bucket")
      .setCreated(bucket.getCreationDate().getTime())
      .setOwner(bucket.getOwner().getDisplayName())
      .setIsDirectory(true)
      .build();
  }

  public static S3ObjectInfo ofDir(String dir) {
    String[] parts = dir.split("/");
    String name = dir;
    if (parts.length > 1) {
      name = parts[parts.length - 1];
    }
    return builder(name, "directory").setPath(dir).setIsDirectory(true).build();
  }

  public static S3ObjectInfo ofObject(S3ObjectSummary summary, FileTypeDetector detector) {
    int idx = summary.getKey().lastIndexOf("/");
    String name = summary.getKey();
    if (idx != -1) {
      name = name.substring(idx + 1);
    }
    String type = detector.detectFileType(name);
    boolean canWrangle = detector.isWrangleable(type);
    return builder(name, type)
      .setPath(summary.getKey())
      .setOwner(summary.getOwner().getDisplayName())
      .setStorageClass(summary.getStorageClass())
      .setLastModified(summary.getLastModified().getTime())
      .setSize(summary.getSize())
      .setIsDirectory(false)
      .setCanWrangle(canWrangle)
      .build();
  }

  public static Builder builder(String name, String type) {
    return new Builder(name, type);
  }

  /**
   * Builds S3ObjectInfo instances.
   */
  public static class Builder {
    private final String name;
    private final String type;
    private String path;
    private String owner;
    private String storageClass;
    private Long created;
    private Long lastModified;
    private Long size;
    private Boolean isDirectory;
    private Boolean canWrangle;

    private Builder(String name, String type) {
      this.name = name;
      this.type = type;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder setStorageClass(String storageClass) {
      this.storageClass = storageClass;
      return this;
    }

    public Builder setCreated(long created) {
      this.created = created;
      return this;
    }

    public Builder setLastModified(long lastModified) {
      this.lastModified = lastModified;
      return this;
    }

    public Builder setSize(long size) {
      this.size = size;
      return this;
    }

    public Builder setIsDirectory(boolean isDirectory) {
      this.isDirectory = isDirectory;
      return this;
    }

    public Builder setCanWrangle(boolean canWrangle) {
      this.canWrangle = canWrangle;
      return this;
    }

    public S3ObjectInfo build() {
      return new S3ObjectInfo(name, type, path, owner, storageClass, created, lastModified, size,
                              isDirectory, canWrangle);
    }
  }
}
