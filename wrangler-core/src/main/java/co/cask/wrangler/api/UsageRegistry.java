package co.cask.wrangler.api;

import co.cask.wrangler.steps.ExtractRegexGroups;
import co.cask.wrangler.steps.JsPath;
import co.cask.wrangler.steps.XmlToJson;
import co.cask.wrangler.steps.column.Columns;
import co.cask.wrangler.steps.column.ColumnsReplace;
import co.cask.wrangler.steps.column.Copy;
import co.cask.wrangler.steps.column.Drop;
import co.cask.wrangler.steps.column.Keep;
import co.cask.wrangler.steps.column.Merge;
import co.cask.wrangler.steps.column.Rename;
import co.cask.wrangler.steps.column.SplitToColumns;
import co.cask.wrangler.steps.column.Swap;
import co.cask.wrangler.steps.date.FormatDate;
import co.cask.wrangler.steps.nlp.Stemming;
import co.cask.wrangler.steps.parser.CsvParser;
import co.cask.wrangler.steps.parser.FixedLengthParser;
import co.cask.wrangler.steps.parser.HL7Parser;
import co.cask.wrangler.steps.parser.JsonParser;
import co.cask.wrangler.steps.parser.ParseDate;
import co.cask.wrangler.steps.parser.ParseLog;
import co.cask.wrangler.steps.row.Flatten;
import co.cask.wrangler.steps.row.RecordConditionFilter;
import co.cask.wrangler.steps.row.RecordMissingOrNullFilter;
import co.cask.wrangler.steps.row.RecordRegexFilter;
import co.cask.wrangler.steps.row.SplitToRows;
import co.cask.wrangler.steps.transformation.CatalogLookup;
import co.cask.wrangler.steps.transformation.CharacterCut;
import co.cask.wrangler.steps.transformation.Expression;
import co.cask.wrangler.steps.transformation.FillNullOrEmpty;
import co.cask.wrangler.steps.transformation.GenerateUUID;
import co.cask.wrangler.steps.transformation.IndexSplit;
import co.cask.wrangler.steps.transformation.Lower;
import co.cask.wrangler.steps.transformation.MaskNumber;
import co.cask.wrangler.steps.transformation.MaskShuffle;
import co.cask.wrangler.steps.transformation.MessageHash;
import co.cask.wrangler.steps.transformation.Quantization;
import co.cask.wrangler.steps.transformation.Sed;
import co.cask.wrangler.steps.transformation.Split;
import co.cask.wrangler.steps.transformation.SplitEmail;
import co.cask.wrangler.steps.transformation.TitleCase;
import co.cask.wrangler.steps.transformation.Upper;
import co.cask.wrangler.steps.transformation.UrlDecode;
import co.cask.wrangler.steps.transformation.UrlEncode;
import co.cask.wrangler.steps.writer.WriteAsJsonMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of directive usages managed through this class.
 */
public final class UsageRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(UsageRegistry.class);

  public class UsageDatum {
    private final String usage;
    private final String description;

    public UsageDatum(String usage, String description) {
      this.usage = usage;
      this.description = description;
    }

    public String getUsage() {
      return usage;
    }

    public String getDescription() {
      return description;
    }
  }

  // Mapping of specification usages.
  private final Map<String, UsageDatum> usages = new HashMap<>();

  // List of all registered directive implementations defined as steps in the pipeline.
  private final List<Class<? extends AbstractStep>> stepRegistry = Arrays.asList (
    CharacterCut.class,
    Columns.class,
    Copy.class,
    CsvParser.class,
    Drop.class,
    Expression.class,
    FillNullOrEmpty.class,
    FixedLengthParser.class,
    Flatten.class,
    FormatDate.class,
    GenerateUUID.class,
    HL7Parser.class,
    IndexSplit.class,
    JsonParser.class,
    JsPath.class,
    Keep.class,
    Lower.class,
    MaskNumber.class,
    MaskShuffle.class,
    Merge.class,
    MessageHash.class,
    ParseDate.class,
    ParseLog.class,
    Quantization.class,
    RecordConditionFilter.class,
    RecordRegexFilter.class,
    Rename.class,
    Sed.class,
    Split.class,
    SplitEmail.class,
    SplitToColumns.class,
    SplitToRows.class,
    Swap.class,
    TitleCase.class,
    Upper.class,
    UrlDecode.class,
    UrlEncode.class,
    XmlToJson.class,
    WriteAsJsonMap.class,
    RecordMissingOrNullFilter.class,
    CatalogLookup.class,
    Stemming.class,
    ColumnsReplace.class,
    ExtractRegexGroups.class
  );

  public UsageRegistry() {
    // Iterate through registry of steps to collect the
    // directive and usage.
    for (Class<? extends AbstractStep> step : stepRegistry) {
      Usage usage = step.getAnnotation(Usage.class);
      if (usage == null) {
        LOG.warn("Usage annotation for directive '{}' missing.", step.getSimpleName());
        continue;
      }
      usages.put(usage.directive(), new UsageDatum(usage.usage(), usage.description()));
    }

    // These are for directives that use other steps for executing.
    // we add them exclusively
    usages.put("set format",
               new UsageDatum("set format csv <delimiter> <skip empty lines>",
                              "[DEPRECATED] Parses the predefined column as CSV. Use 'parse-as-csv'."));
    usages.put("format-unix-timestamp",
               new UsageDatum("format-unix-timestamp <column> <format>",
                              "Formats a unix timestamp using the format specified."));
  }

  /**
   * Gets the usage of a directive.
   *
   * @param directive for which usage is returned.
   * @return null if not found, else the usage.
   */
  public String getUsage(String directive) {
    if(usages.containsKey(directive)) {
      return usages.get(directive).getUsage();
    }
    return null;
  }

  /**
   * Gets the description of a directive.
   *
   * @param directive for which usage is returned.
   * @return null if not found, else the description of usage.
   */
  public String getDescription(String directive) {
    if(usages.containsKey(directive)) {
      return usages.get(directive).getDescription();
    }
    return null;
  }

  /**
   * @return A map of directive to {@link UsageDatum}.
   */
  public Map<String, UsageDatum> getAll() {
    return usages;
  }
}
