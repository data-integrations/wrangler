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

package co.cask.wrangler.utils;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link ObjectSerDe}
 */
public class ObjectSerDeTest {

  @Test
  public void testSerDe() throws Exception {
    ObjectSerDe<List<Record>> objectSerDe = new ObjectSerDe<>();
    List<Record> records = new ArrayList<>();
    records.add(new Record("bytes", "foo".getBytes(Charsets.UTF_8)).add("a", 1).add("b", 2.0));
    records.add(new Record("bytes", "boo".getBytes(Charsets.UTF_8)).add("a", 2).add("b", 3.0));
    byte[] bytes = objectSerDe.toByteArray(records);
    List<Record> newRecords = objectSerDe.toObject(bytes);
    Assert.assertEquals(records.size(), newRecords.size());
    Assert.assertEquals(records.get(0).getColumn(0), newRecords.get(0).getColumn(0));
    Assert.assertEquals(records.get(0).getColumn(1), newRecords.get(0).getColumn(1));
    Assert.assertEquals(records.get(0).getColumn(2), newRecords.get(0).getColumn(2));
  }

  @Test
  public void testNull() throws Exception {
    ObjectSerDe<List<Record>> objectSerDe = new ObjectSerDe<>();
    List<Record> records = new ArrayList<>();
    records.add(new Record("bytes", null));
    records.add(new Record("bytes", null));
    byte[] bytes = objectSerDe.toByteArray(records);
    List<Record> newRecords = objectSerDe.toObject(bytes);
    Assert.assertEquals(records.size(), newRecords.size());
  }

}
