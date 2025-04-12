/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

/**
 * Class description here.
 */
public final class JsonTestData {
  public static final String MALFORMED_BASIC_JSON = "{\n" +
    "  \"a\" : 1,\n" +
    "  \"b\" : 2.0,\n" +
    "  \"c\" : \"test\"\n" +
    "  \"d\" : true\n" +
    "}";
  public static final String BASIC = "{\n" +
    "  \"a\" : 1,\n" +
    "  \"b\" : 2.0,\n" +
    "  \"c\" : \"test\",\n" +
    "  \"d\" : true\n" +
    "}";
  public static final String ARRAY_OF_OBJECTS = "[\n" +
    "  { \"a\" : 1, \"b\" : 2, \"c\" : \"x\" },\n" +
    "  { \"a\" : 2, \"b\" : 3, \"c\" : \"y\" },\n" +
    "  { \"a\" : 3, \"b\" : 4, \"c\" : \"z\" }\n" +
    "]";
  public static final String SIMPLE_JSON_OBJECT = "{\n" +
    "  \"fname\" : \"root\",\n" +
    "  \"lname\" : \"joltie\",\n" +
    "  \"age\" : 20,\n" +
    "  \"weight\" : 182.3,\n" +
    "  \"location\" : \"New York\",\n" +
    "  \"address\" : {\n" +
    "    \"city\" : \"New York\",\n" +
    "    \"state\" : \"New York\",\n" +
    "    \"zip\" : 97474,\n" +
    "    \"gps\" : {\n" +
    "      \"lat\" : 12.23,\n" +
    "      \"long\" : 14.54,\n" +
    "      \"universe\" : {\n" +
    "        \"galaxy\" : \"milky way\",\n" +
    "        \"start\" : \"sun\",\n" +
    "        \"size\" : 24000,\n" +
    "        \"alive\" : true\n" +
    "      }\n" +
    "    }\n" +
    "  }\n" +
    "}";
  public static final String SIMPLE_JSON_OBJECT_CASE_MIX = "{\n" +
    "  \"FNAME\" : \"root\",\n" +
    "  \"lname\" : \"joltie\",\n" +
    "  \"age\" : 20,\n" +
    "  \"weight\" : 182.3,\n" +
    "  \"location\" : \"New York\",\n" +
    "  \"ADDRESS\" : {\n" +
    "    \"city\" : \"New York\",\n" +
    "    \"state\" : \"New York\",\n" +
    "    \"zip\" : 97474,\n" +
    "    \"GPS\" : {\n" +
    "      \"lat\" : 12.23,\n" +
    "      \"long\" : 14.54,\n" +
    "      \"universe\" : {\n" +
    "        \"galaxy\" : \"milky way\",\n" +
    "        \"start\" : \"sun\",\n" +
    "        \"size\" : 24000,\n" +
    "        \"ALIVE\" : true\n" +
    "      }\n" +
    "    }\n" +
    "  }\n" +
    "}";
  public static final String JSON_ARRAY_WITH_OBJECT = "[\n" +
    " {\n" +
    "  \"fname\" : \"root\",\n" +
    "  \"lname\" : \"joltie\",\n" +
    "  \"age\" : 20,\n" +
    "  \"weight\" : 182.3,\n" +
    "  \"location\" : \"New York\",\n" +
    "  \"address\" : {\n" +
    "    \"city\" : \"New York\",\n" +
    "    \"state\" : \"New York\",\n" +
    "    \"zip\" : 97474,\n" +
    "    \"gps\" : {\n" +
    "      \"lat\" : 12.23,\n" +
    "      \"long\" : 14.54,\n" +
    "      \"universe\" : {\n" +
    "        \"galaxy\" : \"milky way\",\n" +
    "        \"start\" : \"sun\",\n" +
    "        \"size\" : 24000,\n" +
    "        \"alive\" : true,\n" +
    "        \"population\" : [ 4,5,6,7,8,9]\n" +
    "      }\n" +
    "    }\n" +
    "  }\n" +
    "}\n" +
    "]";
  public static final String JSON_ARRAY_WITH_OBJECT_CASE_MIX = "[\n" +
    " {\n" +
    "  \"FName\" : \"root\",\n" +
    "  \"LNAME\" : \"joltie\",\n" +
    "  \"age\" : 20,\n" +
    "  \"weight\" : 182.3,\n" +
    "  \"LOCATION\" : \"New York\",\n" +
    "  \"address\" : {\n" +
    "    \"city\" : \"New York\",\n" +
    "    \"state\" : \"New York\",\n" +
    "    \"zip\" : 97474,\n" +
    "    \"gps\" : {\n" +
    "      \"lat\" : 12.23,\n" +
    "      \"long\" : 14.54,\n" +
    "      \"Universe\" : {\n" +
    "        \"galaxy\" : \"milky way\",\n" +
    "        \"start\" : \"sun\",\n" +
    "        \"size\" : 24000,\n" +
    "        \"alive\" : true,\n" +
    "        \"POPULATION\" : [ 4,5,6,7,8,9]\n" +
    "      }\n" +
    "    }\n" +
    "  }\n" +
    "}\n" +
    "]";
  public static final String COMPLEX_1 = "{\n" +
    "  \"numbers\" : [ 1,2,3,4,5,6],\n" +
    "  \"object\" : {\n" +
    "    \"a\" : 1,\n" +
    "    \"b\" : 2,\n" +
    "    \"c\" : [ \"a\", \"b\", \"c\", \"d\" ],\n" +
    "    \"d\" : [ \n" +
    "      { \"a\" : 1 },\n" +
    "      { \"a\" : 2 },\n" +
    "      { \"a\" : 3 }\n" +
    "    ]\n" +
    "  }\n" +
    "}";
  public static final String ARRAY_OF_NUMBERS = "[ 1, 2, 3, 4, 5]";
  public static final String ARRAY_OF_STRING = "[ \"A\", \"B\", \"C\"]";
  public static final String COMPLEX_2 = "{\n" +
    "  \"a\" : [ 1, 2, 3, 4],\n" +
    "  \"b\" : [ \"A\", \"B\", \"C\"],\n" +
    "  \"d\" : true,\n" +
    "  \"e\" : 1,\n" +
    "  \"f\" : \"string\",\n" +
    "  \"g\" : {\n" +
    "    \"g1\" : [ 1, 2, 3, 4],\n" +
    "    \"g2\" : [\n" +
    "      { \"g21\" : 1}\n" +
    "    ]\n" +
    "  }\n" +
    "}";
  public static final String FB_JSON = "{"
    + "\"apiVersion\":\"2.0\", "
    + "\"data\":{ "
    + "  \"updated\":\"2010-01-07T19:58:42.949Z\", "
    + "  \"totalItems\":800, "
    + "  \"startIndex\":1, "
    + "  \"itemsPerPage\":1, "
    + "  \"items\":[ "
    + "    {\"id\":\"hYB0mn5zh2c\","
    + "      \"uploaded\":\"2007-06-05T22:07:03.000Z\","
    + "      \"updated\":\"2010-01-07T13:26:50.000Z\","
    + "      \"uploader\":\"GoogleDeveloperDay\","
    + "      \"category\":\"News\","
    + "      \"title\":\"Google Developers Day US - Maps API Introduction\","
    + "      \"description\":\"Google Maps API Introduction ...\","
    + "      \"tags\":[ \"GDD07\",\"GDD07US\",\"Maps\" ],"
    + "      \"thumbnail\":{"
    + "        \"default\":\"http://i.ytimg.com/vi/hYB0mn5zh2c/default.jpg\","
    + "        \"hqDefault\":\"http://i.ytimg.com/vi/hYB0mn5zh2c/hqdefault.jpg\" },"
    + "        \"player\":{ \"default\":\"http://www.youtube.com/watch?vu003dhYB0mn5zh2c\" },"
    + "        \"content\":{ "
    + "          \"a\":\"rtsp://v5.cache3.c.youtube.com/CiILENy.../0/0/0/video.3gp\","
    + "          \"t\":\"http://www.youtube.com/v/hYB0mn5zh2c?f...\","
    + "          \"y\":\"rtsp://v1.cache1.c.youtube.com/CiILENy.../0/0/0/video.3gp\" },"
    + "          \"duration\":2840, \"aspectRatio\":\"widescreen\", \"rating\":4.63, \"ratingCount\":68,"
    + "          \"viewCount\":220101, \"favoriteCount\":201, \"commentCount\":22, "
    + "          \"status\":{ \"value\":\"restricted\", \"reason\":\"limitedSyndication\" },"
    + "          \"accessControl\":{ "
    + "            \"syndicate\":\"allowed\", \"commentVote\":\"allowed\", \"rate\":\"allowed\", \"list\":\"allowed\","
    + "            \"comment\":\"allowed\", \"embed\":\"allowed\", \"videoRespond\":\"moderated\" }"
    + "       }"
    + "     ]"
    + "   }"
    + " }";
  public static final String EMPTY_OBJECT = "{ \"dividesplitdetails\":{\"type0\":[]}}";
  public static final String NULL_OBJECT = "{ \"dividesplitdetails\":{\"type0\":null, \"type1\":0}}";
}
