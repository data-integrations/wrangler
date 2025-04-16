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
package io.cdap.wrangler.proto.adls;

import javax.annotation.Nullable;

/**
 * Information about an ADLS directory entry.
 */
public class ADLSDirectoryEntryInfo {
  private final String name;
  private final String path;
  private final long displaySize;
  private final String type;
  private final String group;
  private final String user;
  private final String permission;
  private final String modifiedTime;
  private final Boolean directory;
  private final Boolean wrangle;

  private ADLSDirectoryEntryInfo(String name, String type, @Nullable String path, @Nullable String owner,
                                 @Nullable String group,
                                 @Nullable String permission, @Nullable Long displaySize,
                                 @Nullable String modifiedTime,
                                 @Nullable Boolean directory, @Nullable Boolean wrangle) {
    this.name = name;
    this.path = path;
    this.type = type;
    this.user = owner;
    this.group = group;
    this.permission = permission;
    this.displaySize = displaySize;
    this.modifiedTime = modifiedTime;
    this.directory = directory;
    this.wrangle = wrangle;
  }

  public static Builder builder(String name, String type) {
    return new Builder(name, type);
  }

  /**
   * Builds ADLSDirectoryEntryInfo instances.
   */
  public static class Builder {
    private final String name;
    private final String type;
    private String path;
    private String user;
    private String group;
    private Long displaySize;
    private String modifiedTime;
    private String permission;
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

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setGroup(String group) {
      this.group = group;
      return this;
    }

    public Builder setDisplaySize(long displaySize) {
      this.displaySize = displaySize;
      return this;
    }

    public Builder setLastModified(String modifiedTime) {
      this.modifiedTime = modifiedTime;
      return this;
    }

    public Builder setPermission(String permission) {
      this.permission = permission;
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

    public ADLSDirectoryEntryInfo build() {
      return new ADLSDirectoryEntryInfo(name, type, path, user, group, permission, displaySize, modifiedTime,
              isDirectory, canWrangle);
    }
  }
}
