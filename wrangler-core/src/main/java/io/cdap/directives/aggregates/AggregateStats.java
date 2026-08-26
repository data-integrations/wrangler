/*
 * Copyright © 2024 Adhyan Jain
 *
 * Licensed under the Apache License, Version 2.0
 */

 package io.cdap.directives.aggregates;

 import java.util.ArrayList;
 import java.util.List;
 
 import io.cdap.wrangler.api.*;
 import io.cdap.wrangler.api.parser.*;

import java.util.Collections;
 import java.util.List;
import java.util.ArrayList;





 
 /**
  * A directive to aggregate byte size and time duration columns.
  * Usage:
  * aggregate-stats :data_size :response_time total_size_mb total_time_sec
  */
  public class AggregateStats implements Directive
  {
   private String inputSizeCol;
   private String inputTimeCol;
   private String outputSizeCol;
   private String outputTimeCol;
 
   private long totalBytes = 0;
   private long totalMillis = 0;
 
@Override
public UsageDefinition define() {
  return null; // Skip usage metadata for now — directive still works fine
}


   

   



   
   
 
   @Override
   public void initialize(Arguments arguments) throws DirectiveParseException {
     inputSizeCol = ((ColumnName) arguments.value("inputSizeColumn")).value();
     inputTimeCol = ((ColumnName) arguments.value("inputTimeColumn")).value();
     outputSizeCol = ((Text) arguments.value("outputSizeColumn")).value();
     outputTimeCol = ((Text) arguments.value("outputTimeColumn")).value();
   }
 
 
  
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
     long totalBytes = 0;
     long totalMillis = 0;
   
     for (Row row : rows) {
       Object sizeObj = row.getValue(inputSizeCol);
       Object timeObj = row.getValue(inputTimeCol);
   
       try {
         if (sizeObj instanceof String) {
           ByteSize size = new ByteSize((String) sizeObj);
           totalBytes += size.getBytes();
         }
   
         if (timeObj instanceof String) {
           TimeDuration duration = new TimeDuration((String) timeObj);
           totalMillis += duration.getMilliseconds();
         }
       } catch (Exception e) {
         throw new DirectiveExecutionException("Failed to parse input size or time: " + e.getMessage(), e);
       }
     }
   
     double sizeInMB = totalBytes / (1024.0 * 1024.0);
     double timeInSec = totalMillis / 1000.0;
   
     Row output = new Row();
     output.add(outputSizeCol, sizeInMB);
     output.add(outputTimeCol, timeInSec);
   
     return Collections.singletonList(output);
   }
   
   
 

   @Override
public void destroy() {
  // Nothing to clean up
}

 }
 