package co.cask.wrangler.api;

import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
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