// package io.cdap.directives.aggregates;

// import io.cdap.wrangler.api.*;
// import io.cdap.wrangler.api.Row;
// import io.cdap.wrangler.api.Directive;
// import io.cdap.wrangler.api.DirectiveContext;
// import io.cdap.wrangler.api.DirectiveExecutionException;
// import io.cdap.wrangler.api.ExecutorContext;

// import java.util.List;
// import java.util.ArrayList;

// public class AggregateStats implements Directive {
//     private String byteSizeColumn;
//     private String timeColumn;
//     private String targetByteSizeCol;
//     private String targetTimeCol;
//     private String sizeUnit = "MB";    // Optional
//     private String timeUnit = "seconds"; // Optional
//     private boolean average = false;

//     private long totalSizeBytes = 0;
//     private long totalTimeNanos = 0;
//     private int rowCount = 0;

//     @Override
//     public UsageDefinition define() {
//         return UsageDefinition.builder()
//             .define("byteColumn", ColumnName.class)
//             .define("timeColumn", ColumnName.class)
//             .define("targetByteColumn", ColumnName.class)
//             .define("targetTimeColumn", ColumnName.class)
//             .defineOptional("sizeUnit", Text.class)      // Optional args
//             .defineOptional("timeUnit", Text.class)
//             .defineOptional("aggregation", Text.class)   // "total" or "average"
//             .build();
//     }

//     @Override
//     public void initialize(Arguments args) {
//         this.byteSizeColumn = ((ColumnName) args.value("byteColumn")).value();
//         this.timeColumn = ((ColumnName) args.value("timeColumn")).value();
//         this.targetByteSizeCol = ((ColumnName) args.value("targetByteColumn")).value();
//         this.targetTimeCol = ((ColumnName) args.value("targetTimeColumn")).value();

//         if (args.has("sizeUnit")) {
//             this.sizeUnit = ((Text) args.value("sizeUnit")).value().toUpperCase();
//         }
//         if (args.has("timeUnit")) {
//             this.timeUnit = ((Text) args.value("timeUnit")).value().toLowerCase();
//         }
//         if (args.has("aggregation")) {
//             this.average = ((Text) args.value("aggregation")).value().equalsIgnoreCase("average");
//         }
//     }

//     @Override
//     public List<Row> execute(List<Row> rows, ExecutorContext context)
//             throws DirectiveExecutionException {
        
//         // Aggregate values
//         for (Row row : rows) {
//             Object byteVal = row.getValue(byteSizeColumn);
//             Object timeVal = row.getValue(timeColumn);

//             long bytes = ByteSize.parse(byteVal.toString()).getBytes();
//             long nanos = TimeDuration.parse(timeVal.toString()).getNanoSeconds();

//             totalSizeBytes += bytes;
//             totalTimeNanos += nanos;
//             rowCount++;
//         }

//         // Compute final values
//         double finalSize = average ? totalSizeBytes / (double) rowCount : totalSizeBytes;
//         double finalTime = average ? totalTimeNanos / (double) rowCount : totalTimeNanos;

//         // Convert units
//         double convertedSize = convertBytes(finalSize, sizeUnit);
//         double convertedTime = convertTime(finalTime, timeUnit);

//         List<Row> result = new ArrayList<>();
//         Row row = new Row();
//         row.add(targetByteSizeCol, convertedSize);
//         row.add(targetTimeCol, convertedTime);
//         result.add(row);
//         return result;
//     }

//     private double convertBytes(double bytes, String unit) {
//         switch (unit) {
//             case "KB": return bytes / 1024;
//             case "MB": return bytes / (1024 * 1024);
//             case "GB": return bytes / (1024 * 1024 * 1024);
//             default: return bytes;
//         }
//     }

//     private double convertTime(double nanos, String unit) {
//         switch (unit) {
//             case "ms":
//             case "millis":
//             case "milliseconds": return nanos / 1_000_000.0;
//             case "seconds": return nanos / 1_000_000_000.0;
//             case "minutes": return nanos / 60_000_000_000.0;
//             default: return nanos;
//         }
//     }
// }
