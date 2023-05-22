/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.parser;

import com.google.common.io.Closeables;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.functions.Types;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A step to parse Excel files.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-as-excel")
@Categories(categories = { "parser", "excel"})
@Description("Parses column as Excel file.")
public class ParseExcel implements Directive, Lineage {
  public static final String NAME = "parse-as-excel";
  private static final Logger LOG = LoggerFactory.getLogger(ParseExcel.class);
  private String column;
  private String sheet;
  private boolean firstRowAsHeader = false;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("sheet", TokenType.TEXT, Optional.TRUE);
    builder.define("first-row-as-header", TokenType.BOOLEAN, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    if (args.contains("sheet")) {
      this.sheet = ((Text) args.value("sheet")).value();
    } else {
      this.sheet = "0";
    }
    if (args.contains("first-row-as-header")) {
      this.firstRowAsHeader = ((Boolean) args.value("first-row-as-header").value());
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> records, final ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    List<Row> results = new ArrayList<>();
    ByteArrayInputStream input = null;
    DataFormatter formatter = new DataFormatter();
    try {
      for (Row record : records) {
        int idx = record.find(column);
        if (idx != -1) {
          Object object = record.getValue(idx);
          byte[] bytes = null;
          if (object instanceof byte[]) {
            bytes = (byte[]) object;
          } else if (object instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) object;
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
          } else {
            throw new DirectiveExecutionException(
              NAME, String.format("Column '%s' should be of type 'byte array' or 'ByteBuffer'.", column));
          }

          if (bytes != null) {
            input = new ByteArrayInputStream(bytes);
            XSSFWorkbook book = new XSSFWorkbook(input);
            XSSFSheet excelsheet;
            if (Types.isInteger(sheet)) {
              excelsheet = book.getSheetAt(Integer.parseInt(sheet));
            } else {
              excelsheet = book.getSheet(sheet);
            }

            if (excelsheet == null) {
              throw new DirectiveExecutionException(
                NAME, String.format("Failed to extract sheet '%s' from the excel. " +
                                      "Sheet '%s' does not exist.", sheet, sheet));
            }

            Map<Integer, String> columnNames = new TreeMap<>();
            Iterator<org.apache.poi.ss.usermodel.Row> it = excelsheet.iterator();
            int rows = 0;
            while (it.hasNext()) {
              org.apache.poi.ss.usermodel.Row row = it.next();
              Iterator<Cell> cellIterator = row.cellIterator();
              if (checkIfRowIsEmpty(row)) {
                continue;
              }

              Row newRow = new Row();
              newRow.add("fwd", rows);

              while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                String name = columnName(cell.getAddress().getColumn());
                if (firstRowAsHeader && rows > 0) {
                  String value = columnNames.get(cell.getAddress().getColumn());
                  if (value != null) {
                    name = value;
                  }
                }
                String value = "";
                switch (cell.getCellTypeEnum()) {
                  case STRING:
                    value = cell.getStringCellValue();
                    break;

                  case NUMERIC:
                    if (HSSFDateUtil.isCellDateFormatted(cell)) {
                      value = formatter.formatCellValue(cell);
                    } else {
                      value = String.valueOf(cell.getNumericCellValue());
                    }
                    break;

                  case BOOLEAN:
                    value = String.valueOf(cell.getBooleanCellValue());
                    break;
                }
                newRow.add(name, value);

                if (rows == 0 && firstRowAsHeader) {
                  columnNames.put(cell.getAddress().getColumn(), value);
                }
              }

              if (firstRowAsHeader && rows == 0) {
                rows++;
                continue;
              }

              // add old columns to the new row
              for (Pair<String, Object> field : record.getFields()) {
                String colName = field.getFirst();
                // if new row does not contain this column and this column is not the blob column that contains
                // the excel data.
                if (newRow.getValue(colName) == null && !colName.equals(column)) {
                  newRow.add(colName, field.getSecond());
                }
              }
              results.add(newRow);
              rows++;
            }

            if (firstRowAsHeader) {
              rows = rows - 1;
            }

            for (int i = rows - 1; i >= 0; --i) {
              results.get(rows - i - 1).addOrSetAtIndex(1, "bkd", i); // fwd - 0, bkd - 1.
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ErrorRowException(NAME, e.getMessage(), 1);
    } finally {
      if (input != null) {
        Closeables.closeQuietly(input);
      }
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Parsed column '%s' as a Excel", column)
      .all(Many.columns(column))
      .build();
  }

  private boolean checkIfRowIsEmpty(org.apache.poi.ss.usermodel.Row row) {
    if (row == null) {
      return true;
    }
    if (row.getLastCellNum() <= 0) {
      return true;
    }
    for (int cellNum = row.getFirstCellNum(); cellNum < row.getLastCellNum(); cellNum++) {
      Cell cell = row.getCell(cellNum);
      if (cell != null && cell.getCellTypeEnum() != CellType.BLANK && StringUtils.isNotBlank(cell.toString())) {
        return false;
      }
    }
    return true;
  }

  private String columnName(int number) {
    final StringBuilder sb = new StringBuilder();

    int num = number;
    while (num >=  0) {
      int numChar = (num % 26)  + 65;
      sb.append((char) numChar);
      num = (num  / 26) - 1;
    }
    return sb.reverse().toString();
  }
}
