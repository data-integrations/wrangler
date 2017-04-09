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

package co.cask.wrangler.steps.parser;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.AbstractSchemaGenerator;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.RecordConvertorException;
import co.cask.wrangler.steps.PipelineTest;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests {@link JsParser}
 */
public class JsParserTest {

  @Test
  public void testParseJsonAndJsonPath() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body",
      "parse-as-csv  body_deviceReference_screenSize | false",
      "drop body_deviceReference_screenSize",
      "rename body_deviceReference_screenSize_1 size1",
      "rename body_deviceReference_screenSize_2 size2",
      "rename body_deviceReference_screenSize_3 size3",
      "rename body_deviceReference_screenSize_4 size4",
      "json-path body_deviceReference_alerts signal_lost $.[*].['Signal lost']",
      "json-path signal_lost signal_lost $.[0]",
      "drop body",
      "rename body_deviceReference_timestamp timestamp",
      "set column timestamp timestamp",
      "drop body_deviceReference_alerts"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "{ \"deviceReference\": { \"brand\": \"Samsung \", \"type\": \"Gear S3 frontier\", " +
        "\"deviceId\": \"SM-R760NDAAXAR\", \"timestamp\": 122121212341231, \"OS\": { \"name\": \"Tizen OS\", " +
        "\"version\": \"2.3.1\" }, \"alerts\": [ { \"Signal lost\": true }, { \"Emergency call\": true }, " +
        "{ \"Wifi connection lost\": true }, { \"Battery low\": true }, { \"Calories\": 354 } ], \"screenSize\": " +
        "\"extra-small|small|medium|large\", \"battery\": \"22%\", \"telephoneNumber\": \"+14099594986\", \"comments\": " +
        "\"It is an AT&T samung wearable device.\" } }")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 1);
  }

  @Test
  public void testArrayOfObjects() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "[ { \"a\" : 1, \"b\" : 2 }, { \"a\" : 3, \"b\" : 3 }, { \"a\" : 4, \"c\" : 5 } ]")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 3);
  }

  @Test
  public void testArrayOfNumbers() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "[1,2,3,4,5]")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 5);
  }


  @Test
  public void testFlattenAllTheWay() throws Exception {
    String[] directives = new String[]{
      "parse-as-json body 1",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "{ \"deviceReference\": { \"brand\": \"Samsung \", \"type\": \"Gear S3 frontier\", " +
        "\"deviceId\": \"SM-R760NDAAXAR\", \"timestamp\": 122121212341231, \"OS\": { \"name\": \"Tizen OS\", " +
        "\"version\": \"2.3.1\" }, \"alerts\": [ { \"Signal lost\": true }, { \"Emergency call\": true }, " +
        "{ \"Wifi connection lost\": true }, { \"Battery low\": true }, { \"Calories\": 354 } ], \"screenSize\": " +
        "\"extra-small|small|medium|large\", \"battery\": \"22%\", \"telephoneNumber\": \"+14099594986\", \"comments\": " +
        "\"It is an AT&T samung wearable device.\" } }")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 1);
  }

  @Test
  public void testFlattenToDepth() throws Exception {
    String[] directives = new String[]{
      "parse-as-json body 2"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "{\"created_at\":\"Mon Feb 06 21:13:37 +0000 2017\",\"id\":828713281267367937," +
        "\"id_str\":\"828713281267367937\",\"text\":\"Youth is counted sweetest by those who are no longer young." +
        "\\n#PBBPADALUCKKISSYONG\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" " +
        "rel=\\\"nofollow\\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\"" +
        ":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_" +
        "reply_to_screen_name\":null,\"user\":{\"id\":520417116,\"id_str\":\"520417116\",\"name\":\"glenda andres\"," +
        "\"screen_name\":\"glenda_andres\",\"location\":\"Israel\",\"url\":null,\"description\":\"volleyball,badminton\"," +
        "\"protected\":false,\"verified\":false,\"followers_count\":12,\"friends_count\":94,\"listed_count\":1," +
        "\"favourites_count\":42,\"statuses_count\":595,\"created_at\":\"Sat Mar 10 13:57:16 +0000 2012\",\"utc_offset" +
        "\":null,\"time_zone\":null,\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator" +
        "\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\" +
        "/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images" +
        "\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"1DA1F2\",\"profile_" +
        "sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\"," +
        "\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\" +
        "/680735812972052480\\/hsvuZASG_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_" +
        "images\\/680735812972052480\\/hsvuZASG_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_" +
        "banners\\/520417116\\/1451511037\",\"default_profile\":true,\"default_profile_image\":false,\"following\":null," +
        "\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null," +
        "\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":" +
        "{\"hashtags\":[{\"text\":\"PBBPADALUCKKISSYONG\",\"indices\":[60,80]}],\"urls\":[],\"user_mentions\":[]," +
        "\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\"" +
        ":\"1486415617659\"}"));

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 1);
  }

  private static final String BASIC = "{\n" +
    "  \"a\" : 1,\n" +
    "  \"b\" : 2.0,\n" +
    "  \"c\" : \"test\",\n" +
    "  \"d\" : true\n" +
    "}";

  private static final String ARRAY_OF_OBJECTS = "[\n" +
    "  { \"a\" : 1, \"b\" : 2, \"c\" : \"x\" },\n" +
    "  { \"a\" : 2, \"b\" : 3, \"c\" : \"y\" },\n" +
    "  { \"a\" : 3, \"b\" : 4, \"c\" : \"z\" }\n" +
    "]";

  private static final String SIMPLE_JSON_OBJECT = "{\n" +
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

  private static final String JSON_ARRAY_WITH_OBJECT = "[\n" +
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


  private static final String COMPLEX_1 = "{\n" +
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

  private static final String ARRAY_OF_NUMBERS = "[ 1, 2, 3, 4, 5]";
  private static final String ARRAY_OF_STRING = "[ \"A\", \"B\", \"C\"]";
  private static final String COMPLEX_2 = "{\n" +
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
  private static final String JSON_COMPLEX = "{\"version\":null,\"warnings\":null,\"errors\":null,\"reservation\":{\"numberinparty\":\"1\",\"numberofinfants\":0,\"numberinsegment\":1,\"hostoperatedcodeshare\":true,\"hostmarketedcodeshare\":null,\"istrippnr\":null,\"ismulticarrierplatform\":null,\"bookingdetails\":{\"header\":[],\"recordlocator\":\"PCBALL\",\"creationtimestamp\":1485233940000,\"systemcreationtimestamp\":1485233940000,\"creationagentid\":null,\"updatetimestamp\":1485233999000,\"pnrsequence\":\"3\",\"flightsrange\":{\"start\":1487186400000,\"end\":1487254200000},\"itinerarydate\":null,\"dividesplitdetails\":{\"type0\":[]},\"estimatedpurgetimestamp\":1487203200000,\"updatetoken\":\"-50eb96f98caf5467123e2d677331795cd65a000177aac26d\"},\"pos\":{\"source\":{\"bookingsource\":\"A0A0\",\"agentsine\":null,\"pseudocitycode\":\"TTY\",\"isocountry\":\"KW\",\"isocurrency\":null,\"agentdutycode\":null,\"airlinevendorid\":\"9W\",\"airportcode\":null,\"firstdepartpoint\":null,\"sourcesystem\":null,\"terminalid\":null,\"homepseudocitycode\":\"TTY\",\"requestorid\":null,\"bookingchannel\":null,\"ttyrecordlocator\":[{\"crslocator\":\"HDQ\",\"crscode\":\"EY\",\"recordlocator\":\"GCCWNR\",\"agencyid\":\"3R1G\",\"iatanumber\":\"42210195\",\"agencylocation\":\"KWI\",\"usertype\":\"T\",\"countrycode\":\"KW\",\"currency\":null,\"dutycode\":null,\"erspuserid\":null,\"firstpointofdeparture\":null}],\"oac\":null}},\"passengerreservation\":{\"passengers\":{\"numberinparty\":null,\"corporate\":null,\"blockedspacegroup\":null,\"zgroup\":null,\"passenger\":[{\"id\":\"5\",\"nametype\":\"S\",\"passengertype\":null,\"referencenumber\":null,\"nameid\":\"01.01\",\"nameassocid\":\"1\",\"withinfant\":null,\"op\":null,\"lang\":null,\"elementid\":\"pnr-5.1\",\"lastname\":\"S*****\",\"originallastname\":null,\"firstname\":\"SADASDMR\",\"originalfirstname\":null,\"title\":null,\"frequentflyer\":[],\"emailaddress\":[],\"gender\":null,\"dateofbirth\":null,\"profiles\":null,\"specialrequests\":{\"genericspecialrequest\":[],\"childrequest\":[],\"apisrequest\":[{\"docaentry\":null,\"docoentry\":null,\"docsentry\":{\"id\":\"14\",\"type\":\"A\",\"op\":null,\"documenttype\":\"P\",\"countryofissue\":\"AU\",\"documentnumber\":\"SADASD\",\"documentnationalitycountry\":\"AZ\",\"dateofbirth\":\"1992-10-15\",\"gender\":\"M\",\"documentexpirationdate\":\"2029-10-15\",\"surname\":\"SADASD\",\"forename\":\"SADASDMR\",\"middlename\":\"\",\"primaryholder\":false,\"freetext\":\"\",\"actioncode\":\"HK\",\"numberinparty\":\"1\",\"vendorcode\":\"9W\"}}],\"emergencycontactrequest\":[],\"specialmealrequest\":[],\"passportinfomessage\":[],\"seatrequest\":[],\"unaccompaniedminormessage\":[],\"wheelchairrequest\":[],\"ticketingrequest\":[]},\"seats\":{\"prereservedseats\":null,\"seatspecialrequests\":null},\"prereservedseats\":null,\"accountinglines\":null,\"passengerprofileid\":null,\"ancillaryservices\":null,\"osi\":null,\"remarks\":null,\"phonenumbers\":null,\"ticketinginfo\":null,\"fqtvupgraderequests\":null,\"openreservationelements\":null}]},\"segments\":{\"poc\":{\"airport\":\"KUL\",\"departure\":1487186400000},\"air\":[],\"vehicle\":[],\"product\":[],\"hotel\":[],\"open\":[],\"arunk\":[],\"general\":[],\"segment\":[{\"sequence\":1,\"id\":\"8\",\"air\":{\"index\":null,\"id\":\"8\",\"sequence\":1,\"segmentassociationid\":2,\"op\":null,\"ispast\":false,\"departureairport\":\"KUL\",\"departureairportcodecontext\":\"IATA\",\"departureterminalname\":null,\"departureterminalcode\":null,\"arrivalairport\":\"AUH\",\"arrivalairportcodecontext\":\"IATA\",\"arrivalterminalname\":null,\"arrivalterminalcode\":null,\"operatingairlinecode\":\"EY\",\"operatingairlineshortname\":\"ETIHAD AIRWAYS\",\"operatingflightnumber\":\"0411\",\"equipmenttype\":\"332\",\"marketingairlinecode\":\"EY\",\"marketingflightnumber\":\"0411\",\"operatingclassofservice\":\"L\",\"marketingclassofservice\":\"L\",\"codeshareoperatingrecordlocator\":null,\"marriagegrp\":{\"ind\":\"0\",\"group\":\"0\",\"sequence\":\"0\"},\"marriedconnectionindicator\":null,\"seats\":{\"prereservedseats\":null,\"seatspecialrequests\":null},\"airlinerefid\":null,\"eticket\":true,\"departuredatetime\":\"2017-02-15T19:20:00\",\"arrivaldatetime\":\"2017-02-15T23:00:00\",\"flightnumber\":\"0411\",\"classofservice\":\"L\",\"actioncode\":\"HK\",\"numberinparty\":\"1\",\"segmentspecialrequests\":{\"genericspecialrequest\":[],\"othsfopmessage\":[],\"seatrequest\":[],\"wheelchairrequest\":[]},\"inboundconnection\":false,\"outboundconnection\":true,\"ancillaryservices\":null,\"bsgindicator\":null,\"bsgpnrrecordlocator\":null,\"bsgpnrcreateddate\":null,\"passivesegmentindicator\":null,\"schedulechangeindicator\":false,\"segmentbookeddate\":1485233940000,\"updateddeparturedate\":null,\"updateddeparturetime\":null,\"updatedarrivaltime\":null,\"pos\":null,\"fqtvupgraderequests\":null,\"oddata\":null},\"vehicle\":null,\"hotel\":null,\"open\":null,\"arunk\":null,\"general\":null,\"product\":null},{\"sequence\":2,\"id\":\"10\",\"air\":{\"index\":null,\"id\":\"10\",\"sequence\":2,\"segmentassociationid\":3,\"op\":null,\"ispast\":false,\"departureairport\":\"AUH\",\"departureairportcodecontext\":\"IATA\",\"departureterminalname\":null,\"departureterminalcode\":null,\"arrivalairport\":\"HYD\",\"arrivalairportcodecontext\":\"IATA\",\"arrivalterminalname\":null,\"arrivalterminalcode\":null,\"operatingairlinecode\":\"9W\",\"operatingairlineshortname\":\"JET AIRWAYS\",\"operatingflightnumber\":\"0549\",\"equipmenttype\":\"73H\",\"marketingairlinecode\":\"EY\",\"marketingflightnumber\":\"8701\",\"operatingclassofservice\":\"K\",\"marketingclassofservice\":\"L\",\"codeshareoperatingrecordlocator\":null,\"marriagegrp\":{\"ind\":\"0\",\"group\":\"0\",\"sequence\":\"0\"},\"marriedconnectionindicator\":null,\"seats\":{\"prereservedseats\":null,\"seatspecialrequests\":null},\"airlinerefid\":null,\"eticket\":true,\"departuredatetime\":\"2017-02-16T09:00:00\",\"arrivaldatetime\":\"2017-02-16T14:10:00\",\"flightnumber\":\"0549\",\"classofservice\":\"K\",\"actioncode\":\"HK\",\"numberinparty\":\"1\",\"segmentspecialrequests\":{\"genericspecialrequest\":[],\"othsfopmessage\":[],\"seatrequest\":[],\"wheelchairrequest\":[]},\"inboundconnection\":true,\"outboundconnection\":false,\"ancillaryservices\":null,\"bsgindicator\":null,\"bsgpnrrecordlocator\":null,\"bsgpnrcreateddate\":null,\"passivesegmentindicator\":null,\"schedulechangeindicator\":false,\"segmentbookeddate\":1485233940000,\"updateddeparturedate\":null,\"updateddeparturetime\":null,\"updatedarrivaltime\":null,\"pos\":{\"crscode\":null,\"iatanumber\":\"0\",\"agencycitycode\":\"TTY\",\"countrycode\":null,\"dutycode\":\"R\",\"oacaccountingcitycode\":null,\"oacaccountingcode\":null},\"fqtvupgraderequests\":null,\"oddata\":{\"uniqueid\":\"00000000-0000-0000-0000-000000000000\",\"originalfare\":0,\"effectivebidprice\":0,\"adjustedfare\":0,\"adjustedbidprice\":0,\"fareclass\":null,\"lowestavailfareclass\":null,\"pointofcommencement\":null,\"customerscore\":\"0\",\"frequentflyer\":null,\"market\":null,\"tripmarket\":null,\"retrievedfaremarket\":null,\"faretypeindicators\":{\"regular\":false,\"sponsor\":false,\"default\":false,\"sponsordefault\":false,\"hostdefault\":false},\"evaluationtypeindicators\":{\"default\":false,\"sumoflocals\":false,\"trueod\":false,\"longestsegmentfare\":false,\"dominantsegmentfare\":false,\"local\":false,\"financial\":false,\"physical\":false},\"exceptiontypeindicators\":{\"forcedsell\":true,\"schedchg\":false,\"forcedpartialcancel\":false,\"ttyrejectagent\":false,\"ttytransaction\":true},\"additionalflags\":\"0\",\"additionaldata\":null}},\"vehicle\":null,\"hotel\":null,\"open\":null,\"arunk\":null,\"general\":null,\"product\":null}]},\"formsofpayment\":null,\"ticketinginfo\":{\"futureticketing\":[],\"ticketingtimelimit\":[],\"tourticketing\":[],\"alreadyticketed\":[],\"eticketnumber\":[],\"ticketdetails\":[]},\"itinerarypricing\":{\"numberinparty\":null,\"futurepriceinfo\":[],\"priceditinerary\":[]}},\"bsgreservation\":null,\"reservationvaluescore\":null,\"dknumbers\":null,\"corporatedids\":null,\"receivedfrom\":{\"frompassenger\":null,\"name\":\"HDQBBEY241059 12916E79-001\",\"agentname\":null,\"tourwholesalerpcc\":null,\"newcontrollingpcc\":null},\"addresses\":null,\"phonenumbers\":null,\"remarks\":null,\"emailaddresses\":{\"emailaddress\":[]},\"accountinglines\":null,\"genericspecialrequests\":[{\"id\":\"6\",\"type\":\"A\",\"msgtype\":\"O\",\"op\":null,\"code\":\"CTCT\",\"freetext\":\"KWI ABTT 00965 22956660\",\"actioncode\":null,\"numberinparty\":null,\"airlinecode\":\"9W\",\"ticketnumber\":null,\"fulltext\":\"9W CTCT KWI ABTT 00965 22956660\"},{\"id\":\"7\",\"type\":\"A\",\"msgtype\":\"O\",\"op\":null,\"code\":null,\"freetext\":\"BKS LOCATOR INFO SWI1G 53TJ4Y - DO NOT DELETE\",\"actioncode\":null,\"numberinparty\":null,\"airlinecode\":\"9W\",\"ticketnumber\":null,\"fulltext\":\"9W BKS LOCATOR INFO SWI1G 53TJ4Y - DO NOT DELETE\"},{\"id\":\"12\",\"type\":\"A\",\"msgtype\":\"O\",\"op\":null,\"code\":null,\"freetext\":\"PAX CTC 009658888888888 REF SADASD\",\"actioncode\":null,\"numberinparty\":null,\"airlinecode\":\"9W\",\"ticketnumber\":null,\"fulltext\":\"9W PAX CTC 009658888888888 REF SADASD\"},{\"id\":null,\"type\":null,\"msgtype\":null,\"op\":null,\"code\":null,\"freetext\":null,\"actioncode\":null,\"numberinparty\":null,\"airlinecode\":null,\"ticketnumber\":null,\"fulltext\":null}],\"actions\":null,\"profiles\":null,\"associationmatrices\":null,\"reservationextensions\":null,\"history\":null,\"openreservationelements\":null},\"content\":null,\"vcr\":null}";
  private static final String EMPTY_OBJECT = "{ \"dividesplitdetails\":{\"type0\":[]}}";

  private static final String[] TESTS = new String[] {
    BASIC,
    SIMPLE_JSON_OBJECT,
    ARRAY_OF_OBJECTS,
    JSON_ARRAY_WITH_OBJECT,
    COMPLEX_1,
    ARRAY_OF_NUMBERS,
    ARRAY_OF_STRING,
    COMPLEX_2,
    EMPTY_OBJECT
//    JSON_COMPLEX
  };

  @Test
  public void convertSimpleJson() throws Exception {
    int i = 1;
    for (String test : TESTS) {
//      String file = String.format("/Users/nitin/Downloads/schema_%d.avsc", i);
      Schema schema = toSchema("myrecord", test);
//      FileUtils.writeStringToFile(new File(file), schema.toString());
//      System.out.println(schema.toString());
//      System.out.println("\n\n");
      i++;
    }
  }

  private Schema toSchema(String name, String json) throws RecordConvertorException {
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(json);
    return toSchema(name, element);
  }

  private Schema toSchema(String name, JsonElement element) throws RecordConvertorException {
    Schema schema = null;
    if (element.isJsonObject() || element.isJsonArray()) {
      schema = toComplexSchema(name, element);
    } else if (element.isJsonPrimitive()) {
      schema = toSchema(name, element.getAsJsonPrimitive());
    }
    Schema output = Schema.nullableOf(schema);
    while(output.isNullable()) {
      output = output.getNonNullable();
    }
    return Schema.recordOf("record", Schema.Field.of(name, output));
  }

  private Schema toSchema(String name, JsonArray array) throws RecordConvertorException {
    int[] types = new int[3];
    types[0] = types[1] = types[2] = 0;
    for(int i = 0; i < array.size(); ++i) {
      JsonElement item = array.get(i);
      if (item.isJsonArray()) {
        types[1]++;
      } else if (item.isJsonPrimitive()) {
        types[0]++;
      } else if (item.isJsonObject()) {
        types[2]++;
      }
    }

    int sum = types[0] + types[1] + types[2];
    if (sum != array.size()) {
      throw new RecordConvertorException("Problem with array.");
    }

    if (sum > 0) {
      JsonElement child = array.get(0);
      if (types[2] > 0 || types[1] > 0) {
        return Schema.nullableOf(Schema.arrayOf(toComplexSchema(name, child)));
      } else if (types[0] > 0) {
        return Schema.nullableOf(Schema.arrayOf(toSchema(name, child.getAsJsonPrimitive())));
      }
    }
    return Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.NULL)));
  }

  private Schema toSchema(String name, JsonPrimitive primitive) throws RecordConvertorException {
    Object value = JsParser.getValue(primitive);
    try {
      return Schema.nullableOf(new SimpleSchemaGenerator().generate(value.getClass()));
    } catch (UnsupportedTypeException e) {
      throw new RecordConvertorException(
        String.format("Unable to convert field '%s' to basic type.", name)
      );
    }
  }

  private Schema toSchema(String name, JsonObject object) throws RecordConvertorException {
    List<Schema.Field> fields = new ArrayList<>();
    Iterator<Map.Entry<String, JsonElement>> iterator = object.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry<String, JsonElement> next = iterator.next();
      String key = next.getKey();
      JsonElement child = next.getValue();
      Schema schema = null;
      if (child.isJsonObject() || child.isJsonArray()) {
        schema = toComplexSchema(key, child);
      } else if (child.isJsonPrimitive()) {
        schema = toSchema(key, child.getAsJsonPrimitive());
      }
      if (schema != null) {
        fields.add(Schema.Field.of(key, schema));
      }
    }
    System.out.println("Schema record name " + name);
    return Schema.recordOf(name, fields);
  }

  private Schema toComplexSchema(String name, JsonElement element) throws RecordConvertorException {
    if (element.isJsonObject()) {
      return toSchema(name, element.getAsJsonObject());
    } else if (element.isJsonArray()){
      return toSchema(name, element.getAsJsonArray());
    } else if (element.isJsonPrimitive()) {
      return toSchema(name, element.getAsJsonPrimitive());
    }
    return null;
  }


  private static final class SimpleSchemaGenerator extends AbstractSchemaGenerator {
    @Override
    protected Schema generateRecord(TypeToken<?> typeToken, Set<String> set,
                                    boolean b) throws UnsupportedTypeException {
      // we don't actually leverage this method for types we support, so no need to implement it
      throw new UnsupportedTypeException(String.format("Generating record of type %s is not supported.",
                                                       typeToken));
    }
  }

}