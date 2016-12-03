package co.cask.wrangler.Stages;

import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.Row;
import co.cask.wrangler.WrangleStepException;

/**
 * A CSV Parser Stage for parsing the {@link Row} provided based on configuration.
 */
public class CsvParser implements WrangleStep {
  private Options options;
  private String col;

  public CsvParser(Options options, String col) {
    this.options = options;
    this.col = col;
  }

  @Override
  public Row execute(Row row) throws WrangleStepException {
    return null;
  }

  /**
   * Specifies the configuration for the CSV parser.
   */
  public class Options {
    private String delimiter;
    private boolean ignoreEmptyLines;
    private boolean allowMissingColumnNames;
    private String recordSeparator;
    private boolean ignoreSurroundingSpaces;
    private char commentMarker;
  }
}
