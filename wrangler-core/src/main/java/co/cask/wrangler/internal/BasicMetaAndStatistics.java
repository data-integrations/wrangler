package co.cask.wrangler.internal;

import co.cask.wrangler.api.ColumnMetrics;
import co.cask.wrangler.api.MetaAndStatistics;
import co.cask.wrangler.api.Record;
import io.dataapps.chlorine.finder.FinderEngine;
import net.minidev.json.JSONObject;
import org.json.JSONArray;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by nitin on 2/4/17.
 */
public class BasicMetaAndStatistics implements MetaAndStatistics {
  private final FinderEngine engine;
  private final Record meta;

  public BasicMetaAndStatistics() {
    engine = new FinderEngine("wrangler-finder.xml", true, false);
    meta = new Record();
  }

  @Override
  public void aggregate(List<Record> records) {
    ColumnMetrics types = new ColumnMetrics();
    ColumnMetrics stats = new ColumnMetrics();

    Double count = new Double(0);
    for (Record record : records) {
      ++count;
      for (int i = 0; i < record.length(); ++i) {
        String column = record.getColumn(i);
        Object object = record.getValue(i);

        if (object == null) {
          stats.increment(column, "null");
        } else {
          stats.increment(column, "non-null");
        }

        if (object instanceof Integer) {
          types.increment(column, "INTEGER");
        } else if (object instanceof Float) {
          types.increment(column, "REAL");
        } else if (object instanceof Double) {
          types.increment(column, "REAL");
        } else if (object instanceof Date) {
          types.increment(column, "DATE");
        } else if (object instanceof JSONObject) {
          types.increment(column, "JSON");
        } else if (object instanceof JSONArray) {
          types.increment(column, "JSON");
        } else if (object instanceof Short) {
          types.increment(column, "INTEGER" );
        } else if (object instanceof String) {
          String value = ((String) object);
          if (value.isEmpty()) {
            stats.increment(column, "empty");
          } else {
            Map<String, List<String>> finds = engine.findWithType(value);
            for (String find : finds.keySet()) {
              types.increment(column, find);
            }
          }
        }
      }
    }
    meta.add("types", types);
    meta.add("stats", stats);
    meta.add("total", count);
  }

  @Override
  public Record summary() {
    Record recordTypes = new Record();
    ColumnMetrics types = (ColumnMetrics) meta.getValue("types");
    for (String column : types.getColumns()) {
      recordTypes.add(column, types.percentage(column, (Double)meta.getValue("total")));
    }

    Record recordStats = new Record();
    ColumnMetrics stats = (ColumnMetrics) meta.getValue("stats");
    for (String column : stats.getColumns()) {
      recordStats.add(column, stats.percentage(column, (Double)meta.getValue("total")));
    }

    Record record = new Record();
    record.add("types", recordTypes);
    record.add("stats", recordStats);
    record.add("total", meta.getValue("total"));

    return record;
  }
}

