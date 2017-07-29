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
import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;

@Usage(
        directive = "rowhash",
        usage = "rowhash <column> <codec>",
        description = "Returns a hash value for the contents of the entire record"
)
public class RowHash extends AbstractStep {
    private final String column;
    private final String codec;

    public RowHash(int lineno, String directive, String column, String codec) {
        super(lineno, directive);
        this.column = column;
        this.codec = codec.toLowerCase();
    }

    /**
     * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
     *
     * @param records  Input {@link Record} to be wrangled by this step.
     * @param context {@link PipelineContext} passed to each step.
     * @return Wrangled {@link Record}.
     */
    @Override
    public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
        for (Record record : records) {
            switch (codec) {
                case "md5": {
                    String md5 = DigestUtils.md5Hex(record.toString()).toUpperCase();
                    record.addOrSet(column, md5);
                }
                break;

                case "sha1": {
                    String sha1 = DigestUtils.sha1Hex(record.toString()).toUpperCase();
                    record.addOrSet(column, sha1);
                }
                break;

                case "sha256": {
                    String sha256 = DigestUtils.sha256Hex(record.toString()).toUpperCase();
                    record.addOrSet(column, sha256);
                }
                break;

                case "sha384": {
                    String sha384 = DigestUtils.sha384Hex(record.toString()).toUpperCase();
                    record.addOrSet(column, sha384);
                }
                break;

                case "sha512": {
                    String sha512 = DigestUtils.sha512Hex(record.toString()).toUpperCase();
                    record.addOrSet(column, sha512);
                }
                break;

                default: {
                    String md5 = DigestUtils.md5Hex(record.toString()).toUpperCase();
                    record.addOrSet(column, md5);
                }
            }
        }
        return records;
    }
}
