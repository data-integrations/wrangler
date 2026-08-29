/*
 *  Copyright © 2021 Cask Data, Inc.
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

package io.cdap.directives.transformation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ConvertStringTest {

    @Test
    public void testConvertString() throws RecipeException, DirectiveParseException, DirectiveLoadException {
        String[] directives = new String[] {
            "convert-string :col1",
        };

        List<Row> rows = Arrays.asList(
            new Row("col1", 1),
            new Row("col1", 2),
            new Row("col1", 3)
        );

        rows = TestingRig.execute(directives, rows);

        Assert.assertEquals(3, rows.size());
        Assert.assertEquals("1", rows.get(0).getValue("col1"));
        Assert.assertEquals("2", rows.get(1).getValue("col1"));
        Assert.assertEquals("3", rows.get(2).getValue("col1"));
    }
} 

