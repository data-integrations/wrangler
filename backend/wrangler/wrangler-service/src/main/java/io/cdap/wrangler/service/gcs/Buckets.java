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

package io.cdap.wrangler.service.gcs;

import com.google.cloud.storage.Bucket;

import java.util.Collection;

/**
 * Holds GCS {@link Bucket} and whether number of buckets added has exceeded the limit. This is a workaround for
 * CDAP-14446 and should be removed once pagination is implemented.
 */
public class Buckets {
  private Collection<Bucket> buckets;
  private boolean limitExceeded;

  public Buckets(Collection<Bucket> buckets, boolean limitExceeded) {
    this.buckets = buckets;
    this.limitExceeded = limitExceeded;
  }

  public Collection<Bucket> getBuckets() {
    return buckets;
  }

  public boolean isLimitExceeded() {
    return limitExceeded;
  }
}
