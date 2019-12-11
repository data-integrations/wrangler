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
package io.cdap.wrangler.utils;

import org.apache.avro.Schema;
import org.apache.commons.collections4.SetValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;


public class AvroSchemaGlossaryTest {
  private static final String AVRO_STR = "{\n"
      + "    \"type\": \"record\",\n"
      + "    \"name\": \"TEST_6_0_0\",\n"
      + "    \"namespace\": \"google.com.datamodels.test\",\n"
      + "    \"_revision\": \"1\",\n"
      + "    \"fields\": []\n"
      + "}";
  private static final String REVISION_TWO_AVRO_STR = "{\n"
      + "    \"type\": \"record\",\n"
      + "    \"name\": \"TEST_6_0_0\",\n"
      + "    \"namespace\": \"google.com.datamodels.test\",\n"
      + "    \"_revision\": \"2\",\n"
      + "    \"fields\": []\n"
      + "}";
  private static final String DUMMY_SCHEMA = "{\n"
      + "    \"type\": \"record\",\n"
      + "    \"name\": \"TEST_6_0_1\",\n"
      + "    \"namespace\": \"google.com.datamodels.test\",\n"
      + "    \"_revision\": \"1\",\n"
      + "    \"fields\": []\n"
      + "}";
  private static final String SCHEMA_NAME = "google.com.datamodels.test.TEST_6_0_0";
  private static final String DUMMY_NAME = "google.com.datamodels.test.TEST_6_0_1";

  private class StubSchemaLoader implements AvroSchemaLoader {

    StubSchemaLoader(){}

    @Override
    public SetValuedMap<String, Schema> load() throws IOException {
      return new HashSetValuedHashMap<>();
    }
  }

  @Test
  public void testConfigure_schema_successulLoad() throws IOException {
    StubSchemaLoader loader = Mockito.mock(StubSchemaLoader.class);
    Mockito.when(loader.load()).thenReturn(new HashSetValuedHashMap<>());
    AvroSchemaGlossary glossary = new AvroSchemaGlossary(loader);
    Assert.assertTrue(glossary.configure());
  }

  @Test
  public void testConfigure_schema_failureLoading() throws IOException {
    StubSchemaLoader loader = Mockito.mock(StubSchemaLoader.class);
    Mockito.doThrow(new IOException()).when(loader).load();

    AvroSchemaGlossary glossary = new AvroSchemaGlossary(loader);
    Assert.assertFalse(glossary.configure());
  }

  @Test
  public void testGet_schema_successful() throws IOException {
    Schema.Parser parser = new Schema.Parser().setValidate(false);
    SetValuedMap<String, Schema> expected = new HashSetValuedHashMap<>();
    expected.put(SCHEMA_NAME, parser.parse(AVRO_STR));

    StubSchemaLoader loader = Mockito.mock(StubSchemaLoader.class);
    Mockito.when(loader.load()).thenReturn(expected);

    AvroSchemaGlossary glossary = new AvroSchemaGlossary(loader);
    Assert.assertTrue(glossary.configure());
    Assert.assertEquals(SCHEMA_NAME, glossary.get(SCHEMA_NAME, 1L).getFullName());
  }

  @Test
  public void testGet_schema_notFoundInGlossary() throws IOException {
    SetValuedMap<String, Schema> expected = new HashSetValuedHashMap<>();
    StubSchemaLoader loader = Mockito.mock(StubSchemaLoader.class);
    Mockito.when(loader.load()).thenReturn(expected);

    AvroSchemaGlossary glossary = new AvroSchemaGlossary(loader);
    Assert.assertTrue(glossary.configure());
    Assert.assertNull(glossary.get(SCHEMA_NAME, 0L));
  }

  @Test
  public void testGet_schema_unknownVersion() throws IOException {
    Schema.Parser parser = new Schema.Parser().setValidate(false);
    SetValuedMap<String, Schema> expected = new HashSetValuedHashMap<>();
    expected.put(SCHEMA_NAME, parser.parse(AVRO_STR));

    StubSchemaLoader loader = Mockito.mock(StubSchemaLoader.class);
    Mockito.when(loader.load()).thenReturn(expected);

    AvroSchemaGlossary glossary = new AvroSchemaGlossary(loader);
    Assert.assertTrue(glossary.configure());
    Assert.assertNull(glossary.get(SCHEMA_NAME, 99L));
  }

  @Test
  public void testGetAll_schema_populatedCollection() throws IOException {
    Schema.Parser parser = new Schema.Parser().setValidate(false);
    SetValuedMap<String, Schema> expected = new HashSetValuedHashMap<>();
    expected.put(SCHEMA_NAME, parser.parse(AVRO_STR));
    expected.put(SCHEMA_NAME, new Schema.Parser().setValidate(false).parse(REVISION_TWO_AVRO_STR));
    expected.put(DUMMY_NAME, parser.parse(DUMMY_SCHEMA));

    StubSchemaLoader loader = Mockito.mock(StubSchemaLoader.class);
    Mockito.when(loader.load()).thenReturn(expected);

    AvroSchemaGlossary glossary = new AvroSchemaGlossary(loader);
    Assert.assertTrue(glossary.configure());
    Assert.assertEquals(3, glossary.getAll().size());
  }

  @Test
  public void testGetAll_multipleRevisions_populatedCollection() throws IOException {
    SetValuedMap<String, Schema> expected = new HashSetValuedHashMap<>();
    expected.put(SCHEMA_NAME, new Schema.Parser().setValidate(false).parse(AVRO_STR));
    expected.put(SCHEMA_NAME, new Schema.Parser().setValidate(false).parse(REVISION_TWO_AVRO_STR));

    StubSchemaLoader loader = Mockito.mock(StubSchemaLoader.class);
    Mockito.when(loader.load()).thenReturn(expected);

    AvroSchemaGlossary glossary = new AvroSchemaGlossary(loader);
    Assert.assertTrue(glossary.configure());
    Assert.assertEquals(2, glossary.getAll().size());
  }
}
