package co.cask.wrangler.api;

import co.cask.cdap.api.dataset.lib.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by nitin on 2/4/17.
 */
public final class ColumnMetrics {
  private final Map<String, Metrics> measures = new TreeMap<>();

  public void increment(String column, String name) {
    Metrics metric;
    if (measures.containsKey(column)) {
      metric = measures.get(column);
    } else {
      metric = new Metrics();
    }
    metric.increment(name);
    measures.put(column, metric);
  }

  public void set(String column, String name, double value) {
    Metrics metric;
    if (measures.containsKey(column)) {
      metric = measures.get(column);
    } else {
      metric = new Metrics();
    }
    metric.set(name, value);
    measures.put(column, metric);
  }

  public Set<String> getColumns() {
    return measures.keySet();
  }

  public List<KeyValue<String, Double>> percentage(String column, Double sum) {
    if (measures.containsKey(column)) {
      return measures.get(column).percentage(sum);
    }
    return new ArrayList<>();
  }
}
