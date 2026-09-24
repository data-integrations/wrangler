package io.cdap.wrangler.api;

import io.cdap.wrangler.api.Directive;
import java.util.List;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.UsageDefinition.Builder;
import io.cdap.wrangler.api.parser.Token;
import java.util.ArrayList;


public class AggregateDirective implements Directive {
    private String sourceSizeCol;
    private String sourceTimeCol;
    private String targetSizeCol;
    private String targetTimeCol;
    private String outputSizeUnit = "B";
    private String outputTimeUnit = "ms";
    private String aggType = "total";
  
    @Override
    public UsageDefinition define() {
      return UsageDefinition.builder()
        .define("sourceSizeCol", TokenType.COLUMN_NAME)
        .define("sourceTimeCol", TokenType.COLUMN_NAME)
        .define("targetSizeCol", TokenType.COLUMN_NAME)
        .define("targetTimeCol", TokenType.COLUMN_NAME)
        .define("outputSizeUnit", TokenType.TEXT, true)
        .define("outputTimeUnit", TokenType.TEXT, true)
        .define("aggType", TokenType.TEXT, true)
        .build();
    }
  
    @Override
    public void initialize(ExecutorContext context, List<Token> args) throws Exception {
      sourceSizeCol = ((String) args.get(0).value());
      sourceTimeCol = ((String) args.get(1).value());
      targetSizeCol = ((String) args.get(2).value());
      targetTimeCol = ((String) args.get(3).value());
  
      if (args.size() > 4) outputSizeUnit = args.get(4).value().toString();
      if (args.size() > 5) outputTimeUnit = args.get(5).value().toString();
      if (args.size() > 6) aggType = args.get(6).value().toString();
  
      context.getTransientStore().put("totalBytes", 0.0);
      context.getTransientStore().put("totalTime", 0.0);
      context.getTransientStore().put("count", 0);
    }
  
    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
      double totalBytes = context.getTransientStore().<Double>get("totalBytes");
      double totalTime = context.getTransientStore().<Double>get("totalTime");
      int count = context.getTransientStore().<Integer>get("count");
  
      for (Row row : rows) {
        Object sizeVal = row.getValue(sourceSizeCol);
        Object timeVal = row.getValue(sourceTimeCol);
  
        double bytes = sizeVal instanceof ByteSize ? ((ByteSize) sizeVal).getBytes() : new ByteSize(sizeVal.toString()).getBytes();
        double millis = timeVal instanceof TimeDuration ? ((TimeDuration) timeVal).getMilliseconds() : new TimeDuration(timeVal.toString()).getMilliseconds();
  
        totalBytes += bytes;
        totalTime += millis;
        count++;
      }
  
      context.getTransientStore().put("totalBytes", totalBytes);
      context.getTransientStore().put("totalTime", totalTime);
      context.getTransientStore().put("count", count);
  
      return new ArrayList<>();
    }
  
    @Override
    public List<Row> complete(List<Row> rows, ExecutorContext context) throws Exception {
      double totalBytes = context.getTransientStore().<Double>get("totalBytes");
      double totalTime = context.getTransientStore().<Double>get("totalTime");
      int count = context.getTransientStore().<Integer>get("count");
  
      if ("average".equalsIgnoreCase(aggType) && count > 0) {
        totalBytes /= count;
        totalTime /= count;
      }
  
      double finalSize = convertSize(totalBytes, outputSizeUnit);
      double finalTime = convertTime(totalTime, outputTimeUnit);
  
      Row row = new Row();
      row.add(targetSizeCol, finalSize);
      row.add(targetTimeCol, finalTime);
  
      List<Row> result = new ArrayList<>(); 
      result.add(row);
      return result;
    }
  
    private double convertSize(double bytes, String unit) {
      switch (unit.toUpperCase()) {
        case "B": return bytes;
        case "KB": return bytes / 1024;
        case "MB": return bytes / (1024 * 1024);
        case "GB": return bytes / (1024 * 1024 * 1024);
        default: throw new IllegalArgumentException("Unsupported outputSizeUnit: " + unit);
      }
    }
  
    private double convertTime(double millis, String unit) {
      switch (unit.toLowerCase()) {
        case "ms": return millis;
        case "s": return millis / 1000;
        case "m": return millis / (60 * 1000);
        case "h": return millis / (60 * 60 * 1000);
        case "d": return millis / (24 * 60 * 60 * 1000);
        default: throw new IllegalArgumentException("Unsupported outputTimeUnit: " + unit);
      }
    }
  }
  