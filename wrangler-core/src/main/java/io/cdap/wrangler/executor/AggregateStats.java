
 package io.cdap.wrangler.executor;

 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.DirectiveExecutionException;
 import io.cdap.wrangler.api.DirectiveParseException;
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.annotations.Categories;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Identifier;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 
 import java.util.List;
 
 @Categories(categories = {"aggregate"})
 public class AggregateStats implements Directive {
   public static final String NAME = "aggregate-stats";
   private String sizeColumn;
   private String timeColumn;
   private String totalSizeColumn;
   private String totalTimeColumn;
   private long totalBytes;
   private long totalNanoseconds;
   private int rowCount;
 
   @Override
   public UsageDefinition define() {
     UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
     builder.define("size-column", TokenType.COLUMN_NAME);
     builder.define("time-column", TokenType.COLUMN_NAME);
     builder.define("total-size-column", TokenType.COLUMN_NAME);
     builder.define("total-time-column", TokenType.COLUMN_NAME);
     return builder.build();
   }
 
   @Override
   public void initialize(Arguments args) throws DirectiveParseException {
     sizeColumn = ((ColumnName) args.value("size-column")).value();
     timeColumn = ((ColumnName) args.value("time-column")).value();
     totalSizeColumn = ((ColumnName) args.value("total-size-column")).value();
     totalTimeColumn = ((ColumnName) args.value("total-time-column")).value();
     totalBytes = 0;
     totalNanoseconds = 0;
     rowCount = 0;
   }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
     for (Row row : rows) {
       Object sizeValue = row.getValue(sizeColumn);
       Object timeValue = row.getValue(timeColumn);
 
       if (sizeValue instanceof String) {
         totalBytes += new ByteSize((String) sizeValue).getBytes();
       } else if (sizeValue instanceof ByteSize) {
         totalBytes += ((ByteSize) sizeValue).getBytes();
       }
 
       if (timeValue instanceof String) {
         totalNanoseconds += new TimeDuration((String) timeValue).getNanoseconds();
       } else if (timeValue instanceof TimeDuration) {
         totalNanoseconds += ((TimeDuration) timeValue).getNanoseconds();
       }
 
       rowCount++;
     }
 
     if (rowCount == 0) {
       return rows;
     }
 
     Row result = new Row();
     result.add(totalSizeColumn, String.format("%.2fMB", totalBytes / (1024.0 * 1024)));
     result.add(totalTimeColumn, String.format("%.2fs", totalNanoseconds / 1_000_000_000.0));
 
     return List.of(result);
   }
 
   @Override
   public void destroy() {
    
   }
 } 
