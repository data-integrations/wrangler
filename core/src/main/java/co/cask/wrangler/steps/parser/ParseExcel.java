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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.steps.transformation.functions.Types;
import com.google.common.io.Closeables;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A step to parse Excel files.
 */
@Usage(
  directive = "parse-as-excel",
  usage = "parse-as-excel <column> <sheet number | sheet name>",
  description = "Parses column as Excel file."
)
public class ParseExcel extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(ParseExcel.class);
  private final String column;
  private final String sheet;

  public ParseExcel(int lineno, String directive, String column, String sheet) {
    super(lineno, directive);
    this.column = column;
    this.sheet = sheet;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, final PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    ByteArrayInputStream input = null;
    try {
      for (Record record : records) {
        int idx = record.find(column);
        if (idx != -1) {
          Object object = record.getValue(idx);
          if (object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            input = new ByteArrayInputStream(bytes);
            XSSFWorkbook book = new XSSFWorkbook(input);
            XSSFSheet excelsheet;
            if (Types.isInteger(sheet)) {
              excelsheet = book.getSheetAt(Integer.parseInt(sheet));
            } else {
              excelsheet = book.getSheet(sheet);
            }
            Iterator<Row> it = excelsheet.iterator();
            while(it.hasNext()) {
              Row row = it.next();
              Iterator<Cell> cellIterator = row.cellIterator();
              Record newRecord = new Record();
              while (cellIterator.hasNext())  {
                Cell cell = cellIterator.next();
                String name = Integer.toString(cell.getAddress().getColumn());
                switch (cell.getCellTypeEnum()) {
                  case STRING:
                    newRecord.add(name, cell.getStringCellValue());
                    break;

                  case NUMERIC:
                    newRecord.add(name, cell.getNumericCellValue());
                    break;

                  case BOOLEAN:
                    newRecord.add(name, cell.getBooleanCellValue());
                    break;

                  case BLANK:
                    newRecord.add(name, null);
                    break;
                }
              }
              results.add(newRecord);
            }
          } else {
            throw new StepException(toString() + " : column " + column + " is not excel file type.");
          }
        }
      }
    } catch (IOException e) {
      throw new StepException(toString() + " Issue parsing excel file. " + e.getMessage());
    } finally {
      if (input != null) {
        Closeables.closeQuietly(input);
      }
    }
    return results;
  }
}
