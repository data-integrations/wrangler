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

import co.cask.wrangler.service.connections.ConnectionType;
import com.google.gson.Gson;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link Connection}
 */
public class ConnectionTest {

  @Test
  public void testConnectionSerDe() throws Exception {
    Gson gson = new Gson();
    Connection connection = new Connection();
    connection.setName("MySQL Database");
    connection.setType(ConnectionType.DATABASE);
    connection.setDescription("MySQL Config");
    connection.setCreated(System.currentTimeMillis()/1000);
    connection.setUpdated(System.currentTimeMillis()/1000);
    connection.putProp("hostname", "localhost");
    connection.putProp("port", 3306);
    String from = gson.toJson(connection);

    String to = "{\n" +
      "\t\"name\":\"MySQL Database\",\n" +
      "\t\"type\":\"DATABASE\",\n" +
      "\t\"description\":\"MySQL Config\",\n" +
      "\t\"created\":1494505014,\n" +
      "\t\"updated\":1494505014,\n" +
      "\t\"properties\" : {\n" +
      "\t\t\"hostaname\" : \"localhost\",\n" +
      "\t\t\"port\" : 3306\n" +
      "\t}\n" +
      "}";
    Connection newConnection = gson.fromJson(to, Connection.class);
    Number port = newConnection.getProp("port");
    int p = port.intValue();
    Assert.assertEquals(3306, p);
    Assert.assertTrue(true);
  }

  @Test
  public void testStringSub() throws Exception {
    final List<String> variables = new ArrayList<>();
    StrSubstitutor substitutor = new StrSubstitutor(new StrLookup() {
      @Override
      public String lookup(String s) {
        variables.add(s);
        return s;
      }
    });
    substitutor.replace("${username}:${password}");
    Assert.assertEquals(2, variables.size());
  }
}
