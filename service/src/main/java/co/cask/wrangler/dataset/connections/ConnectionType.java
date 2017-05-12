/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.dataset.connections;

/**
 * This class defines different connection types that can be stored in the connections store.
 */
public enum ConnectionType {
  UNDEFINED(-1),
  DATABASE(0),
  KAFKA(1),
  S3(2);

  // Type of the connection defined as int.
  private int type;

  ConnectionType(int type) {
    this.type = type;
  }

  /**
   * @return enum type of the connection.
   */
  public int getType() {
    return type;
  }

  /**
   * From the integer type defines the {@link ConnectionType}
   *
   * @param type id of the type.
   * @return instance of {@link ConnectionType}
   */
  public static ConnectionType from(int type) {
    for(ConnectionType ctype : ConnectionType.values()) {
      if(ctype.getType() == type) {
        return ctype;
      }
    }
    return ConnectionType.UNDEFINED;
  }
}
