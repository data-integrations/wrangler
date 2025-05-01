package io.cdap.wrangler.core;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.Store;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.UsageDefinition;

public class AggregationDirective implements Directive {
  private String sourceSizeColumn;
  private String sourceTimeColumn;
  private String targetSizeColumn;
  private String targetTimeColumn;
  private String sizeUnit = "bytes";  
  private String timeUnit = "nanoseconds";  
  private String aggregationType = "total";  
  private Store store;  

  public AggregationDirective() {

  }

  @Override
  public void define(UsageDefinition definition) {
    this.sourceSizeColumn = definition.getArgument(0);
    this.sourceTimeColumn = definition.getArgument(1);
    this.targetSizeColumn = definition.getArgument(2);
    this.targetTimeColumn = definition.getArgument(3);

    if (definition.getArguments().size() > 4) {
      this.sizeUnit = definition.getArgument(4);  
    }
    if (definition.getArguments().size() > 5) {
      this.timeUnit = definition.getArgument(5);  
    }
    if (definition.getArguments().size() > 6) {
      this.aggregationType = definition.getArgument(6); 
    }
    
    this.store = new Store();  
  }

  @Override
  public void execute(Row row, ExecutorContext context) {

    double byteSize = row.getDouble(sourceSizeColumn);
    double timeDuration = row.getDouble(sourceTimeColumn);
    
    
    store.addTo("totalByteSize", byteSize);
    store.addTo("totalTimeDuration", timeDuration);
  }

  @Override
  public Row finalize(ExecutorContext context) {
    
    double totalByteSize = store.get("totalByteSize");
    double totalTimeDuration = store.get("totalTimeDuration");


    double finalSize = convertSize(totalByteSize);
    double finalTime = convertTime(totalTimeDuration);

    
    Row resultRow = new Row();
    resultRow.add(targetSizeColumn, finalSize);
    resultRow.add(targetTimeColumn, finalTime);

    return resultRow;  
  }

  private double convertSize(double size) {
    switch (sizeUnit) {
      case "MB":
        return size / (1024 * 1024);  
      case "GB":
        return size / (1024 * 1024 * 1024);  
      default:
        return size; 
    }
  }


  private double convertTime(double time) {
    switch (timeUnit) {
      case "seconds":
        return time / 1_000_000_000;  
      case "minutes":
        return time / (1_000_000_000 * 60);  
      default:
        return time;  
    }
  }
}
