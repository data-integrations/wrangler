/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.*;

import java.util.List;

@Usage(
        directive = "reverse",
        usage = "reverse <column>",
        description = "Reverses a string so that it prints backwards"
)
public class Reverse extends AbstractStep {
    // Columns of the column to be upper-cased
    private String col;

    public Reverse(int lineno, String detail, String col) {
        super(lineno, detail);
        this.col = col;
    }

    /**
     * Trimming white spaces from right side of a column value
     *
     * @param records Input {@link Record} to be wrangled by this step.
     * @param context Specifies the context of the pipeline.
     * @return Transformed {@link Record} in which the 'col' value after trimming
     * @throws StepException thrown when type of 'col' is not STRING.
     */
    @Override
    public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
        for (Record record : records) {
            int idx = record.find(col);
            if (idx != -1) {
                Object object = record.getValue(idx);
                if (object instanceof String) {
                    if (object != null) {
                        String value = (String) object;
                        String reversed = new StringBuffer(value).reverse().toString();
                        record.setValue(idx, reversed);
                    }
                }
            }
        }
        return records;
    }
}
