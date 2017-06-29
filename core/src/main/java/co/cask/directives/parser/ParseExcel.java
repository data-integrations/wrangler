/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.functions.Types;
import com.google.common.io.Closeables;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A step to parse Excel files.
 */
@Plugin(type = Directive.Type)
@Name("parse-as-excel")
@Usage("parse-as-excel <column> [<sheet number | sheet name>]")
@Description("Parses column as Excel file.")
public class ParseExcel implements Directive {
  public static final String NAME = "parse-as-excel";
  private static final Logger LOG = LoggerFactory.getLogger(ParseExcel.class);
  private String column;
  private String sheet;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("sheet", TokenType.TEXT, Optional.TRUE);
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
  }

  @Override
  public List<Row> execute(List<Row> records, final RecipeContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    ByteArrayInputStream input = null;
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
            throw new DirectiveExecutionException(toString() + " : column " + column + " is not byte array or byte buffer.");
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
                String.format("Failed to extract sheet '%s' from the excel. Sheet '%s' does not exist.", sheet, sheet)
              );
            }

            int last = excelsheet.getLastRowNum();

            Iterator<org.apache.poi.ss.usermodel.Row> it = excelsheet.iterator();
            int rows = 0;
            while (it.hasNext()) {
              org.apache.poi.ss.usermodel.Row row = it.next();
              Iterator<Cell> cellIterator = row.cellIterator();
              Row newRow = new Row();
              newRow.add("fwd", rows);
              newRow.add("bkd", last - rows - 1);
              while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                String name = columnName(cell.getAddress().getColumn());
                switch (cell.getCellTypeEnum()) {
                  case STRING:
                    newRow.add(name, cell.getStringCellValue());
                    break;

                  case NUMERIC:
                    newRow.add(name, cell.getNumericCellValue());
                    break;

                  case BOOLEAN:
                    newRow.add(name, cell.getBooleanCellValue());
                    break;
                }
              }
              results.add(newRow);
              rows++;
            }
          }
        }
      }
    } catch (IOException e) {
      throw new DirectiveExecutionException(toString() + " Issue parsing excel file. " + e.getMessage());
    } finally {
      if (input != null) {
        Closeables.closeQuietly(input);
      }
    }
    return results;
  }

  private String columnName(int number) {
    final StringBuilder sb = new StringBuilder();

    int num = number;
    while (num >=  0) {
      int numChar = (num % 26)  + 65;
      sb.append((char)numChar);
      num = (num  / 26) - 1;
    }
    return sb.reverse().toString();
  }
}
