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

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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
}