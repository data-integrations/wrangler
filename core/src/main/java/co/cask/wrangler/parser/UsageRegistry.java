/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.steps.IncrementTransientVariable;
import co.cask.wrangler.steps.SetTransientVariable;
import co.cask.wrangler.steps.column.ChangeColCaseNames;
import co.cask.wrangler.steps.column.CleanseColumnNames;
import co.cask.wrangler.steps.column.Columns;
import co.cask.wrangler.steps.column.ColumnsReplace;
import co.cask.wrangler.steps.column.Copy;
import co.cask.wrangler.steps.column.Drop;
import co.cask.wrangler.steps.column.Keep;
import co.cask.wrangler.steps.column.Merge;
import co.cask.wrangler.steps.column.Rename;
import co.cask.wrangler.steps.column.SetType;
import co.cask.wrangler.steps.column.SplitToColumns;
import co.cask.wrangler.steps.column.Swap;
import co.cask.wrangler.steps.date.DiffDate;
import co.cask.wrangler.steps.date.FormatDate;
import co.cask.wrangler.steps.language.SetCharset;
import co.cask.wrangler.steps.nlp.Stemming;
import co.cask.wrangler.steps.parser.CsvParser;
import co.cask.wrangler.steps.parser.FixedLengthParser;
import co.cask.wrangler.steps.parser.HL7Parser;
import co.cask.wrangler.steps.parser.JsParser;
import co.cask.wrangler.steps.parser.JsPath;
import co.cask.wrangler.steps.parser.ParseAvro;
import co.cask.wrangler.steps.parser.ParseAvroFile;
import co.cask.wrangler.steps.parser.ParseDate;
import co.cask.wrangler.steps.parser.ParseExcel;
import co.cask.wrangler.steps.parser.ParseLog;
import co.cask.wrangler.steps.parser.ParseProtobuf;
import co.cask.wrangler.steps.parser.ParseSimpleDate;
import co.cask.wrangler.steps.parser.XmlParser;
import co.cask.wrangler.steps.parser.XmlToJson;
import co.cask.wrangler.steps.row.Fail;
import co.cask.wrangler.steps.row.Flatten;
import co.cask.wrangler.steps.row.RecordConditionFilter;
import co.cask.wrangler.steps.row.RecordMissingOrNullFilter;
import co.cask.wrangler.steps.row.RecordRegexFilter;
import co.cask.wrangler.steps.row.SendToError;
import co.cask.wrangler.steps.row.SetRecordDelimiter;
import co.cask.wrangler.steps.row.SplitToRows;
import co.cask.wrangler.steps.transformation.CatalogLookup;
import co.cask.wrangler.steps.transformation.CharacterCut;
import co.cask.wrangler.steps.transformation.Decode;
import co.cask.wrangler.steps.transformation.Encode;
import co.cask.wrangler.steps.transformation.Expression;
import co.cask.wrangler.steps.transformation.ExtractRegexGroups;
import co.cask.wrangler.steps.transformation.FillNullOrEmpty;
import co.cask.wrangler.steps.transformation.FindAndReplace;
import co.cask.wrangler.steps.transformation.GenerateUUID;
import co.cask.wrangler.steps.transformation.IndexSplit;
import co.cask.wrangler.steps.transformation.InvokeHttp;
import co.cask.wrangler.steps.transformation.LeftTrim;
import co.cask.wrangler.steps.transformation.Lower;
import co.cask.wrangler.steps.transformation.MaskNumber;
import co.cask.wrangler.steps.transformation.MaskShuffle;
import co.cask.wrangler.steps.transformation.MessageHash;
import co.cask.wrangler.steps.transformation.Quantization;
import co.cask.wrangler.steps.transformation.RightTrim;
import co.cask.wrangler.steps.transformation.SetColumn;
import co.cask.wrangler.steps.transformation.Split;
import co.cask.wrangler.steps.transformation.SplitEmail;
import co.cask.wrangler.steps.transformation.SplitURL;
import co.cask.wrangler.steps.transformation.TableLookup;
import co.cask.wrangler.steps.transformation.TextDistanceMeasure;
import co.cask.wrangler.steps.transformation.TextMetricMeasure;
import co.cask.wrangler.steps.transformation.TitleCase;
import co.cask.wrangler.steps.transformation.Trim;
import co.cask.wrangler.steps.transformation.Upper;
import co.cask.wrangler.steps.transformation.UrlDecode;
import co.cask.wrangler.steps.transformation.UrlEncode;
import co.cask.wrangler.steps.transformation.XPathArrayElement;
import co.cask.wrangler.steps.transformation.XPathElement;
import co.cask.wrangler.steps.writer.WriteAsCSV;
import co.cask.wrangler.steps.writer.WriteAsJsonMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of directive usages managed through this class.
 */
public final class UsageRegistry implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(UsageRegistry.class);

  public class UsageDatum {
    private final String directive;
    private final String usage;
    private final String description;

    public UsageDatum(String directive, String usage, String description) {
      this.directive = directive;
      this.usage = usage;
      this.description = description;
    }

    public String getDirective() {
      return directive;
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

  // Listing.
  private final List<UsageDatum> usageList = new ArrayList<>();

  // List of all registered directive implementations defined as steps in the pipeline.
  private final List<Class<? extends AbstractStep>> stepRegistry = Arrays.asList (
    CatalogLookup.class,
    ChangeColCaseNames.class,
    CharacterCut.class,
    CleanseColumnNames.class,
    Columns.class,
    ColumnsReplace.class,
    Copy.class,
    CsvParser.class,
    Decode.class,
    DiffDate.class,
    Drop.class,
    Encode.class,
    Expression.class,
    ExtractRegexGroups.class,
    Fail.class,
    FillNullOrEmpty.class,
    FindAndReplace.class,
    FixedLengthParser.class,
    Flatten.class,
    FormatDate.class,
    GenerateUUID.class,
    HL7Parser.class,
    IndexSplit.class,
    InvokeHttp.class,
    JsParser.class,
    JsPath.class,
    Keep.class,
    Lower.class,
    LeftTrim.class,
    MaskNumber.class,
    MaskShuffle.class,
    Merge.class,
    MessageHash.class,
    ParseAvro.class,
    ParseAvroFile.class,
    ParseProtobuf.class,
    ParseDate.class,
    ParseLog.class,
    ParseSimpleDate.class,
    Quantization.class,
    RecordConditionFilter.class,
    RecordMissingOrNullFilter.class,
    RecordRegexFilter.class,
    Rename.class,
    RightTrim.class,
    SendToError.class,
    SetCharset.class,
    SetColumn.class,
    SetRecordDelimiter.class,
    SetType.class,
    Split.class,
    SplitEmail.class,
    SplitToColumns.class,
    SplitToRows.class,
    SplitURL.class,
    Stemming.class,
    Swap.class,
    TableLookup.class,
    TextDistanceMeasure.class,
    TextMetricMeasure.class,
    TitleCase.class,
    Trim.class,
    Upper.class,
    UrlDecode.class,
    UrlEncode.class,
    WriteAsCSV.class,
    WriteAsJsonMap.class,
    XmlParser.class,
    XmlToJson.class,
    XPathArrayElement.class,
    XPathElement.class,
    ParseExcel.class,
    SetTransientVariable.class,
    IncrementTransientVariable.class
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
      usages.put(usage.directive(), new UsageDatum(usage.directive(), usage.usage(), usage.description()));
      usageList.add(new UsageDatum(usage.directive(), usage.usage(), usage.description()));
    }

    // These are for directives that use other steps for executing.
    // we add them exclusively
    addUsage("set format", "set format csv <delimiter> <skip empty lines>",
             "[DEPRECATED] Parses the predefined column as CSV. Use 'parse-as-csv' instead.");
    addUsage("format-unix-timestamp", "format-unix-timestamp <column> <format>",
             "Formats a UNIX timestamp using the specified format");
    addUsage("filter-row-if-not-matched", "filter-row-if-not-matched <column> <regex>",
             "Filters rows if the regex does not match");
    addUsage("filter-row-if-false", "filter-row-if-false <condition>",
             "Filters rows if the condition evaluates to false");
  }

  private void addUsage(String directive, String usage, String description) {
    UsageDatum d = new UsageDatum(directive, usage, description);
    usages.put(directive, d);
    usageList.add(d);
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
  public List<UsageDatum> getAll() {
    return usageList;
  }
}
