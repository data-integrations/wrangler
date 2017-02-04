package co.cask.wrangler.api;

import co.cask.cdap.api.dataset.lib.KeyValue;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by nitin on 2/4/17.
 */
public final class Metrics {

  private class MutableDouble {
    private double value;

    public MutableDouble() {
      value = 0.0f;
    }

    public MutableDouble(double value) {
      this.value = value;
    }

    public void increment(){
      ++value;
    }
    public double get() {
      return value;
    }
  }
  private final Map<String, MutableDouble> metrics;

  public Metrics() {
    this.metrics = new TreeMap<>();
  }

  public void increment(String name) {
    MutableDouble value = metrics.get(name);
    if (value != null) {
      value.increment();
    } else {
      metrics.put(name, new MutableDouble(1));
    }
  }

  public void set(String name, Double value) {
    metrics.put(name, new MutableDouble(value));
  }

  public double sum() {
    double sum = 0.0;
    for (MutableDouble value : metrics.values()) {
      sum += value.get();
    }
    return sum;
  }

  public double average() {
    double sum = sum();
    return sum / (metrics.size() > 0 ? metrics.size() : 1);
  }

  public KeyValue<String, Double> max() {
    Double max = Double.MIN_VALUE;
    String name = "";
    for (Map.Entry<String, MutableDouble> entry : metrics.entrySet()) {
      if (entry.getValue().get() > max) {
        max = entry.getValue().get();
        name = entry.getKey();
      }
    }
    return new KeyValue<>(name, max);
  }

  public KeyValue<String, Double> min() {
    Double min = Double.MAX_VALUE;
    String name = null;
    for (Map.Entry<String, MutableDouble> entry : metrics.entrySet()) {
      if (entry.getValue().get() < min) {
        min = entry.getValue().get();
        name = entry.getKey();
      }
    }
    return new KeyValue<>(name, min);
  }

}

