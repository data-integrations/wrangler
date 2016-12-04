package co.cask.wrangler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nitin on 12/3/16.
 */
public class Row {
  private static final Logger LOG = LoggerFactory.getLogger(Row.class);
  private List<Object> values = new ArrayList<>();

  public Row() {

  }

  public Row(Column column, String line) {

  }

  public Object get(String col) {
    return null;
  }

  public String getString(String col) {
    return null;
  }

  public void addValue(String s) {
    values.add(s);
  }

  public ColumnType getType(String col) {
    return ColumnType.STRING;
  }

  public void setValue(String col, String value) {

  }
}
