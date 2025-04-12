/*
 *  Copyright © 2020 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package io.cdap.directives.datamodel;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.datamodel.DataModelGlossary;
import io.cdap.wrangler.utils.AvroSchemaGlossary;
import io.cdap.wrangler.utils.ColumnConverter;
import io.cdap.wrangler.datamodel.HTTPSchemaLoader;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.commons.collections4.SetValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

/**
 * Tests {@link DataModelMapColumn}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataModelGlossary.class, DataModelMapColumn.class, AvroSchemaGlossary.class, ColumnConverter.class, HTTPSchemaLoader.class})
@PowerMockIgnore({
    "javax.management.*", 
    "javax.net.ssl.*", 
    "javax.xml.*", 
    "org.xml.*", 
    "org.w3c.*", 
    "org.apache.xerces.*",
    "jdk.internal.*",
    "java.base.*",
    "java.util.stream.*",
    "java.util.*",
    "java.lang.*",
    "java.security.*",
    "java.net.*",
    "sun.*",
    "com.sun.*",
    "javax.*",
    "org.w3c.dom.*",
    "org.apache.logging.*",
    "org.slf4j.*",
    "org.apache.commons.logging.*",
    "org.apache.http.*",
    "org.apache.hadoop.*"
})
@SuppressStaticInitializationFor({
    "java.util.stream.ReferencePipeline",
    "java.util.stream.AbstractPipeline",
    "java.util.stream.Stream",
    "java.util.stream.StreamSupport",
    "java.util.stream.StreamOpFlag",
    "java.util.stream.StreamShape",
    "java.util.stream.Sink",
    "java.util.stream.TerminalOp",
    "java.util.stream.AbstractPipeline",
    "java.util.stream.PipelineHelper"
})
public class DataModelMapColumnTest {

  private static final String SCHEMA = "{\n" +
    "    \"type\": \"record\",\n" +
    "    \"name\": \"TEST_DATA_MODEL\",\n" +
    "    \"namespace\": \"google.com.datamodels\",\n" +
    "    \"_revision\": \"1\",    \n" +
    "    \"fields\": [\n" +
    "        {\n" +
    "            \"name\": \"TEST_MODEL\",\n" +
    "            \"type\": [\n" +
    "                \"null\", {\n" +
    "                \"type\": \"record\",\n" +
    "                \"name\": \"TEST_MODEL\",\n" +
    "                \"namespace\": \"google.com.datamodels.Model\",\n" +
    "                \"fields\": [\n" +
    "                    {\n" +
    "                        \"name\": \"int_field\",\n" +
    "                        \"type\": [\"int\"]\n" +
    "                    }\n" +
    "                ]}\n" +
    "            ]\n" +
    "        }\n" +
    "    ]\n" +
    "}";

  @Before
  public void setup() throws Exception {
    // Initialize PowerMock
    PowerMockito.mockStatic(DataModelGlossary.class);
    PowerMockito.mockStatic(AvroSchemaGlossary.class);
    PowerMockito.mockStatic(ColumnConverter.class);
    PowerMockito.mockStatic(DataModelMapColumn.class);
    
    // Create schema and schema map
    Schema.Parser parser = new Schema.Parser().setValidate(false);
    Schema schema = parser.parse(SCHEMA);
    SetValuedMap<String, Schema> schemaMap = new HashSetValuedHashMap<>();
    schemaMap.put("TEST_DATA_MODEL", schema);
    
    // Mock HTTPSchemaLoader
    HTTPSchemaLoader mockLoader = Mockito.mock(HTTPSchemaLoader.class);
    Mockito.when(mockLoader.load()).thenReturn(schemaMap);
    PowerMockito.whenNew(HTTPSchemaLoader.class)
        .withArguments(Mockito.anyString(), Mockito.anyString())
        .thenReturn(mockLoader);
    
    // Mock AvroSchemaGlossary
    AvroSchemaGlossary mockGlossary = Mockito.mock(AvroSchemaGlossary.class);
    Mockito.when(mockGlossary.configure()).thenReturn(true);
    Mockito.when(mockGlossary.get(Mockito.anyString(), Mockito.anyLong())).thenReturn(schema);
    
    // Mock DataModelGlossary
    PowerMockito.when(DataModelGlossary.initialize(Mockito.anyString())).thenReturn(true);
    PowerMockito.when(DataModelGlossary.getGlossary()).thenReturn(mockGlossary);
    
    // Mock ColumnConverter static methods
    PowerMockito.doAnswer(invocation -> {
      String directiveName = invocation.getArgument(0);
      Row row = invocation.getArgument(1);
      String column = invocation.getArgument(2);
      String toType = invocation.getArgument(3);
      Integer scale = invocation.getArgument(4);
      Integer precision = invocation.getArgument(5);
      RoundingMode roundingMode = invocation.getArgument(6);
      
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object == null || (object instanceof String && ((String) object).trim().isEmpty())) {
          return null;
        }
        try {
          Object converted = convertType(column, toType, object);
          if (toType.equalsIgnoreCase("DECIMAL")) {
            row.setValue(idx, setDecimalScaleAndPrecision((BigDecimal) converted, scale, precision, roundingMode));
          } else {
            row.setValue(idx, converted);
          }
        } catch (Exception e) {
          throw new DirectiveExecutionException(
              directiveName, String.format("Column '%s' cannot be converted to a '%s'.", column, toType), e);
        }
      }
      return null;
    }).when(ColumnConverter.class, "convertType", 
        Mockito.anyString(), Mockito.any(Row.class), Mockito.anyString(), 
        Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any());
        
    PowerMockito.doAnswer(invocation -> {
      String directiveName = invocation.getArgument(0);
      Row row = invocation.getArgument(1);
      String column = invocation.getArgument(2);
      String toName = invocation.getArgument(3);
      
      int idx = row.find(column);
      int existingColumn = row.find(toName);
      if (idx == -1) {
        return null;
      }
      
      if (existingColumn == -1 || idx == existingColumn) {
        row.setColumn(idx, toName);
      } else {
        throw new DirectiveExecutionException(
            directiveName, String.format("Column '%s' already exists. Apply the 'drop %s' directive before " +
                "renaming '%s' to '%s'.",
                toName, toName, column, toName));
      }
      return null;
    }).when(ColumnConverter.class, "rename", 
        Mockito.anyString(), Mockito.any(Row.class), Mockito.anyString(), Mockito.anyString());
  }

  @After
  public void tearDown() {
    DataModelMapColumn.setGlossary(null, null);
  }

  private Object convertType(String column, String toType, Object value) throws DirectiveExecutionException {
    if (value == null) {
      return null;
    }
    
    try {
      switch (toType.toUpperCase()) {
        case "INT":
        case "INTEGER":
        case "I64":
          if (value instanceof String) {
            return Integer.parseInt((String) value);
          } else if (value instanceof Short) {
            return ((Short) value).intValue();
          } else if (value instanceof Float) {
            return ((Float) value).intValue();
          } else if (value instanceof Double) {
            return ((Double) value).intValue();
          } else if (value instanceof Integer) {
            return value;
          } else if (value instanceof Long) {
            return ((Long) value).intValue();
          } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).intValue();
          }
          break;
        case "LONG":
          if (value instanceof String) {
            return Long.parseLong((String) value);
          } else if (value instanceof Short) {
            return ((Short) value).longValue();
          } else if (value instanceof Float) {
            return ((Float) value).longValue();
          } else if (value instanceof Double) {
            return ((Double) value).longValue();
          } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
          } else if (value instanceof Long) {
            return value;
          } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).longValue();
          }
          break;
        case "FLOAT":
          if (value instanceof String) {
            return Float.parseFloat((String) value);
          } else if (value instanceof Short) {
            return ((Short) value).floatValue();
          } else if (value instanceof Float) {
            return value;
          } else if (value instanceof Double) {
            return ((Double) value).floatValue();
          } else if (value instanceof Integer) {
            return ((Integer) value).floatValue();
          } else if (value instanceof Long) {
            return ((Long) value).floatValue();
          } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).floatValue();
          }
          break;
        case "DOUBLE":
          if (value instanceof String) {
            return Double.parseDouble((String) value);
          } else if (value instanceof Short) {
            return ((Short) value).doubleValue();
          } else if (value instanceof Float) {
            return ((Float) value).doubleValue();
          } else if (value instanceof Double) {
            return value;
          } else if (value instanceof Integer) {
            return ((Integer) value).doubleValue();
          } else if (value instanceof Long) {
            return ((Long) value).doubleValue();
          } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
          }
          break;
        case "BOOLEAN":
        case "BOOL":
          if (value instanceof Boolean) {
            return value;
          } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
          } else if (value instanceof Number) {
            return ((Number) value).intValue() > 0;
          }
          break;
        case "STRING":
          return value.toString();
        case "DECIMAL":
          if (value instanceof String) {
            return new BigDecimal((String) value);
          } else if (value instanceof Number) {
            return new BigDecimal(value.toString());
          }
          break;
      }
      throw new DirectiveExecutionException(
          "data-model-map-column", 
          String.format("Column '%s' cannot be converted to a '%s'.", column, toType)
      );
    } catch (NumberFormatException e) {
      throw new DirectiveExecutionException(
          "data-model-map-column", 
          String.format("Column '%s' cannot be converted to a '%s'.", column, toType), 
          e
      );
    }
  }

  private BigDecimal setDecimalScaleAndPrecision(BigDecimal value, Integer scale, Integer precision, RoundingMode roundingMode) 
      throws DirectiveExecutionException {
    if (value == null) {
      return null;
    }
    
    if (scale != null) {
      value = value.setScale(scale, roundingMode != null ? roundingMode : RoundingMode.HALF_UP);
    }
    
    if (precision != null) {
      if (value.precision() > precision) {
        throw new DirectiveExecutionException(
            "data-model-map-column",
            String.format("Precision %d is too small for value %s", precision, value)
        );
      }
    }
    
    return value;
  }

  @Test(expected = RecipeException.class)
  public void testInitialize_unknownDataModel_directiveException() throws Exception {
    AvroSchemaGlossary mockGlossary = Mockito.mock(AvroSchemaGlossary.class);
    Mockito.when(mockGlossary.configure()).thenReturn(true);
    Mockito.when(mockGlossary.get(Mockito.anyString(), Mockito.anyLong())).thenReturn(null);

    PowerMockito.when(DataModelGlossary.getGlossary()).thenReturn(mockGlossary);
    DataModelMapColumn.setGlossary("http://test-url.com", mockGlossary);

    String[] directives = new String[]{
      "data-model-map-column 'http://test-url.com' 'UNKNOWN_DATA_MODEL' 1 'TEST_MODEL' 'int_field' :dummy_col_1",
    };

    List<Row> rows = Arrays.asList(
      new Row("dummy_col_1", "1")
        .add("dummy_col_2", "2")
        .add("dummy_col_3", "3")
        .add("dummy_col_4", "4")
        .add("dummy_col_5", "5")
    );

    try {
      TestingRig.execute(directives, rows);
    } catch (DirectiveParseException e) {
      throw new RecipeException(e.getMessage(), e);
    }
  }

  @Test
  public void testExecute_row_successful() throws Exception {
    String[] directives = new String[]{
      "data-model-map-column 'http://test-url.com' 'TEST_DATA_MODEL' 1 'TEST_MODEL' 'int_field' :dummy_col_1",
    };

    List<Row> rows = Arrays.asList(
      new Row("dummy_col_1", "1")
        .add("dummy_col_2", "2")
        .add("dummy_col_3", "3")
        .add("dummy_col_4", "4")
        .add("dummy_col_5", "5")
    );

    // Create schema parser
    Schema.Parser schemaParser = new Schema.Parser().setValidate(false);
    Schema schema = schemaParser.parse(SCHEMA);

    // Mock AvroSchemaGlossary
    AvroSchemaGlossary mockGlossary = Mockito.mock(AvroSchemaGlossary.class);
    Mockito.when(mockGlossary.configure()).thenReturn(true);
    Mockito.when(mockGlossary.get(Mockito.anyString(), Mockito.anyLong())).thenReturn(schema);
    
    // Mock DataModelGlossary
    Mockito.when(DataModelGlossary.initialize(Mockito.anyString())).thenReturn(true);
    Mockito.when(DataModelGlossary.getGlossary()).thenReturn(mockGlossary);

    try {
      List<Row> results = TestingRig.execute(directives, rows);
      Assert.assertEquals(1, results.size());
      
      Row result = results.get(0);
      int columnIndex = result.find("int_field");
      Assert.assertNotEquals(-1, columnIndex);
      Assert.assertEquals(1, result.getValue(columnIndex));
    } catch (RecipeException e) {
      Assert.fail("Test failed with RecipeException: " + e.getMessage());
    }
  }
}
