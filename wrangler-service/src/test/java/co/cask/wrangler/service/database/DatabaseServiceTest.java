package co.cask.wrangler.service.database;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;

/**
 * Class description here.
 */
public class DatabaseServiceTest {
  private final class DriverInfo {
    private String jdbcUrlPattern;
    private String name;
    private String tag;
    private String port;

    public DriverInfo(String name, String jdbcUrlPattern, String tag, String port) {
      this.name = name;
      this.jdbcUrlPattern = jdbcUrlPattern;
      this.tag = tag;
      this.port = port;
    }

    public String getJdbcUrlPattern() {
      return jdbcUrlPattern;
    }

    public String getName() {
      return name;
    }

    public String getTag() {
      return tag;
    }

    public String getPort() {
      return port;
    }
  }

  @Test
  public void testReadingDriverConfiguration() throws Exception {
    Multimap<String, DriverInfo> drivers = ArrayListMultimap.create();
    InputStream is = DatabaseService.class.getClassLoader().getResourceAsStream("drivers.mapping");
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line;
      while ((line = br.readLine()) != null) {
        String[] columns = line.split(",");
        if (columns.length == 5) {
          DriverInfo info = new DriverInfo(columns[0], columns[2], columns[3], columns[4]);
          drivers.put(columns[1].trim(), info);
        }
      }
      br.close();
      JsonArray values = new JsonArray();
      Collection<Map.Entry<String, DriverInfo>> entries = drivers.entries();
      for (Map.Entry<String, DriverInfo> driver : entries) {
        JsonObject object = new JsonObject();
        object.addProperty("class", driver.getKey());
        object.addProperty("label", driver.getValue().getName());
        String shortTag = driver.getValue().getTag();
        object.addProperty("tag", shortTag);
        object.addProperty("name", shortTag);
        object.addProperty("default.port", driver.getValue().getPort());
        values.add(object);
      }
      Assert.assertEquals(9, values.size());
    } finally {
      if (is != null) {
        is.close();
      }
    }
    Assert.assertEquals(9, drivers.size());
  }
}