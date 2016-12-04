package co.cask.wrangler.steps;

import co.cask.wrangler.Row;
import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.WrangleStepException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A CSV Parser Stage for parsing the {@link Row} provided based on configuration.
 */
public class CsvParser implements WrangleStep {
  private static final Logger LOG = LoggerFactory.getLogger(CsvParser.class);
  private String col;
  private CSVFormat format;

  public CsvParser(Options options, String col) {
    this.col = col;
    this.format = CSVFormat.DEFAULT;
    this.format.withDelimiter(options.delimiter)
      .withIgnoreEmptyLines(options.ignoreEmptyLines)
      .withAllowMissingColumnNames(options.allowMissingColumnNames)
      .withIgnoreSurroundingSpaces(options.ignoreSurroundingSpaces)
      .withRecordSeparator(options.recordSeparator);
  }

  @Override
  public List<Row> execute(List<Row> rows) throws WrangleStepException {
    List<Row> results = new ArrayList<>();
    for(Row row : rows) {
      try {
        String line = row.getString(col);
        CSVParser parser = CSVParser.parse(line, format);
        List<CSVRecord> records = parser.getRecords();
        for (CSVRecord record : records) {
          results.add(toRow(record));
        }
      } catch (IOException e) {
        throw new WrangleStepException(e);
      }
    }
    return results;
  }

  private Row toRow(CSVRecord record) {
    Row row = new Row();
    for ( int i = 0; i < record.size(); i++) {
      row.addValue(record.get(i));
    }
    return row;
  }

  /**
   * Specifies the configuration for the CSV parser.
   */
  public class Options {
    private char delimiter;
    private boolean allowMissingColumnNames;
    private char recordSeparator;
    private boolean ignoreSurroundingSpaces;
    private boolean ignoreEmptyLines;
    private char commentMarker;
  }
}
