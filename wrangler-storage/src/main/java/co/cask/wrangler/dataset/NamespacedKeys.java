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

package co.cask.wrangler.dataset;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.wrangler.proto.NamespacedId;

/**
 * Utility methods around keys for a namespaced entity. The key structure is:
 *
 * [length of namespace][namespace][length of id][id]
 *
 * where the length components are integers and the namespace and id components are byte arrays.
 */
public class NamespacedKeys {

  private NamespacedKeys() {
    // no-op
  }

  public static Scan getScan(String namespace) {
    byte[] namespaceBytes = Bytes.toBytes(namespace);
    byte[] startKey = Bytes.concat(Bytes.toBytes(namespaceBytes.length), namespaceBytes);
    return new Scan(startKey, Bytes.stopKeyForPrefix(startKey));
  }

  /**
   * Returns the rowkey for a contextual id
   * *
   * @param id the contextual id
   * @return the rowkey
   */
  public static byte[] getRowKey(NamespacedId id) {
    byte[] namespaceBytes = Bytes.toBytes(id.getNamespace());
    byte[] idBytes = Bytes.toBytes(id.getId());
    return Bytes.concat(Bytes.toBytes(namespaceBytes.length), namespaceBytes,
                        Bytes.toBytes(idBytes.length), idBytes);
  }

  /**
   * Returns a contextual id from a rowkey generated from {@link #getRowKey(NamespacedId)}.
   *
   * @param rowkey the rowkey
   * @return the decoded contextual id
   */
  public static NamespacedId fromRowKey(byte[] rowkey) {
    int namespaceLength = Bytes.toInt(rowkey, 0);
    String namespace = Bytes.toString(rowkey, Bytes.SIZEOF_INT, namespaceLength);
    int idLength = Bytes.toInt(rowkey, Bytes.SIZEOF_INT + namespaceLength);
    String id = Bytes.toString(rowkey, 2 * Bytes.SIZEOF_INT + namespaceLength, idLength);
    return NamespacedId.of(namespace, id);
  }
}
