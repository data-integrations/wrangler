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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.wrangler.dataset.AbstractTableStore;

/**
 * This class {@link ConnectionStore} manages all the connections defined.
 * It manages the lifecycle of the {@link Connection} including all CRUD operations.
 *
 * Following is the example for usage.
 *
 * <code>
 *  ConnectionStore store = new ConnectionStore(table);
 *  String id = store.create(connection);
 *  if(!store.hasKey(id)) {
 *    Connection connection = store.value(id);
 *    connection.setUpdated(System.currentTimeMillis());
 *    store.update(id, connection);
 *  }
 *  Connection newConnection = store.clone(id);
 *  List<Connection> s = store.scan();
 * </code>
 */
public class ConnectionStore extends AbstractTableStore<Connection> {

  public ConnectionStore(Table table) {
    super(table, Bytes.toBytes("a"), Connection.class);
  }

  /**
   * @return key namespace for all the objects stored in the {@link ConnectionStore}
   */
  @Override
  public String getKeySpace() {
    return "c:";
  }

  /**
   * Creates an entry in the {@link ConnectionStore} for object {@link Connection}.
   *
   * This method creates the id and returns if after successfully updating the store.
   *
   * TODO: (CDAP-14619) pass in a CreateRequest object that doesn't contain created, updated, and id fields.
   *       Also improve error handling. There is currently no way to differentiate an invalid request from
   *       the connection already existing.
   *
   * @param connection to be stored in the store.
   * @return id of the connection stored.
   */
  @Override
  public String create(Connection connection) {
    String name = connection.getName();
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("Name not present for connection.")
      );
    }
    if (connectionExists(name)) {
      throw new IllegalArgumentException(
        String.format("Connection name '%s' already exists.", name)
      );
    }
    String mangled = getConnectionId(name);
    connection.setId(mangled);
    connection.setCreated(now());
    connection.setUpdated(now());
    putObject(Bytes.toBytes(mangled), connection);
    return mangled;
  }

  /**
   * Updates an existing connection in the store.
   *
   * @param id of the object to be updated.
   * @param connection to be updated.
   */
  @Override
  public void update(String id, Connection connection) {
    if (!hasKey(id)) {
      throw new IllegalArgumentException(
        String.format("Connection '%s' does not exists. Create connection before updating", id)
      );
    }
    connection.setUpdated(now());
    updateTable(id, connection);
  }

  /**
   * Clones a connection with the id.
   *
   * @param id of the object to be cloned.
   * @return new instance of connection object.
   */
  @Override
  public Connection clone(String id) {
    Connection connection = get(id);
    String name = connection.getName();
    name = name + "_Clone";
    connection.setId(null);
    connection.setName(name);
    connection.setCreated(now());
    connection.setUpdated(now());
    return connection;
  }

  /**
   * Returns true if connection identified by connectionName already exists
   */
  public boolean connectionExists(String connectionName) {
    String mangled = getConnectionId(connectionName);
    return hasKey(mangled);
  }
}
