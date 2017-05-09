package co.cask.wrangler.clients;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.Key;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class description here.
 */
public class SchemaRegistryClientTest {
  private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  static final JsonFactory JSON_FACTORY = new GsonFactory();

  public static class DailyMotionUrl extends GenericUrl {
    public DailyMotionUrl(String encodedUrl) {
      super(encodedUrl);
    }

    @Key
    public String fields;
  }

  @Test
  public void testUrlConstruction() throws Exception {
    SchemaRegistryClient.DailyMotionUrl url = new SchemaRegistryClient.DailyMotionUrl("https://api.dailymotion.com/videos/");
    HttpRequestFactory requestFactory =
      HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {
        @Override
        public void initialize(HttpRequest request) {
          request.setParser(new JsonObjectParser(JSON_FACTORY));
        }
      });
    url.fields = "id,tags,title,url";
    HttpRequest request = requestFactory.buildGetRequest(url);
    String s = request.execute().parseAsString();
    Assert.assertTrue(true);
  }

}