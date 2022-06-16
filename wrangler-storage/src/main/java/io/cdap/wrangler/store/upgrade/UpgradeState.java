/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.store.upgrade;

import java.util.Objects;

/**
 * Upgrade state to store any upgrade related information. Can be extended to contain more information about the
 * upgrade information
 */
public class UpgradeState {
  // this version is the storage version, if in the future, we want to upgrade the entity type again,
  // we can use this to check what the previous upgraded version is.
  private final long version;

  public UpgradeState(long version) {
    this.version = version;
  }

  public long getVersion() {
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

    UpgradeState that = (UpgradeState) o;
    return version == that.version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(version);
  }
}
