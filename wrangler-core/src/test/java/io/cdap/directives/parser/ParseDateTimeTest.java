/*
 *  Copyright © 2021 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

 package io.cdap.directives.parser;

 import io.cdap.wrangler.TestingRig;
 import io.cdap.wrangler.api.RecipeException;
 import io.cdap.wrangler.api.Row;
 import org.junit.Assert;
 import org.junit.Before;
 import org.junit.Test;
 
 import java.time.*;
 import java.time.format.DateTimeFormatter;
 import java.util.Arrays;
 import java.util.Collections;
 import java.util.List;
 import java.util.TimeZone;

 
 public class ParseDateTimeTest {
 
   @Before
   public void setUp() {
     // Ensure tests run consistently in UTC
     TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
   }
 
   @Test
   public void testDateTimeFormats() throws Exception {
     String[] testPatterns = new String[]{
       "MM/dd/yyyy HH:mm",                            // LocalDateTime
       "yyyy-MM-dd'T'HH:mm:ss",                       // LocalDateTime
       "yyyy-MM-dd'T'HH:mm:ssxxx",                    // OffsetDateTime
       "yyyy-MM-dd'T'HH:mm:ssxxx'['VV']'",            // ZonedDateTime
       "yyyyMMdd h:mm a"                              // LocalDateTime with AM/PM
     };
 
     String[] colNames = new String[]{"col1", "col2", "col3", "col4", "col5"};
 
     String[] dateTimes = new String[]{
       "03/30/2010 01:05",
       "2020-01-28T04:50:12",
       "2011-12-03T10:15:30+01:00",
       "2011-12-03T10:15:30+01:00[Europe/Paris]",
       "19901212 10:12 AM"
     };
 
     String[] directives = new String[testPatterns.length];
     Row row = new Row();
 
     for (int i = 0; i < testPatterns.length; i++) {
       directives[i] = String.format("%s :%s \"%s\"", ParseDateTime.NAME, colNames[i], testPatterns[i]);
       row.add(colNames[i], dateTimes[i]);
     }
 
     List<Row> rows = TestingRig.execute(directives, Collections.singletonList(row));
 
     Assert.assertEquals("Expected exactly one row after execution", 1, rows.size());
     Row resultRow = rows.get(0);
 
     for (int i = 0; i < testPatterns.length; i++) {
       String value = dateTimes[i];
       String pattern = testPatterns[i];
       Object parsedValue = resultRow.getValue(colNames[i]);
 
       DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
 
       if (pattern.contains("VV")) {
         // Timezone present
         ZonedDateTime expected = ZonedDateTime.parse(value, formatter);
         Assert.assertEquals(
           String.format("Mismatch for column '%s' with pattern '%s'", colNames[i], pattern),
           expected,
           parsedValue
         );
       } else if (pattern.contains("xxx")) {
         // Offset present
         OffsetDateTime expected = OffsetDateTime.parse(value, formatter);
         Assert.assertEquals(
           String.format("Mismatch for column '%s' with pattern '%s'", colNames[i], pattern),
           expected,
           parsedValue
         );
       } else {
         // Local date-time
         LocalDateTime expected = LocalDateTime.parse(value, formatter);
         Assert.assertEquals(
           String.format("Mismatch for column '%s' with pattern '%s'", colNames[i], pattern),
           expected,
           parsedValue
         );
       }
     }
   }
 
   @Test
   public void testDateTimeMultipleRows() throws Exception {
     String pattern = "MM/dd/yyyy HH:mm";
     String colName = "col1";
 
     String datetime1 = "12/10/2016 07:45";
     String datetime2 = "02/01/1990 12:01";
 
     DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
 
     String[] directives = new String[]{
       String.format("%s :%s '%s'", ParseDateTime.NAME, colName, pattern)
     };
 
     Row row1 = new Row();
     row1.add(colName, datetime1);
 
     Row row2 = new Row();
     row2.add(colName, datetime2);
 
     List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1, row2));
 
     Assert.assertEquals(2, rows.size());
     Assert.assertEquals(LocalDateTime.parse(datetime1, formatter), rows.get(0).getValue(colName));
     Assert.assertEquals(LocalDateTime.parse(datetime2, formatter), rows.get(1).getValue(colName));
   }
 
   @Test(expected = RecipeException.class)
   public void testInvalidFormat() throws Exception {
     String pattern = "abcd";
     String colName = "col1";
     String datetime1 = "12/10/2016 07:45";
 
     String[] directives = new String[]{
       String.format("parse-datetime :%s '%s'", colName, pattern)
     };
 
     Row row = new Row();
     row.add(colName, datetime1);
 
     TestingRig.execute(directives, Collections.singletonList(row));
   }
 
   @Test
   public void testInvalidData() throws Exception {
     String pattern = "MM/dd/yyyy HH:mm";
     String colName = "col1";
     String invalidDateTime = "12/10/2016"; // Invalid because time is missing
 
     String[] directives = new String[]{
       String.format("%s :%s '%s'", ParseDateTime.NAME, colName, pattern)
     };
 
     Row row = new Row();
     row.add(colName, invalidDateTime);
 
     List<Row> results = TestingRig.execute(directives, Collections.singletonList(row));
 
     Assert.assertTrue("Expected no results for invalid input", results.isEmpty());
   }
 }
 