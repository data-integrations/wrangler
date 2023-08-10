/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler;

import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;


public class WranglerTransformTest {

    @Test(expected = RuntimeException.class)
    public void relationalTransformTest () throws Exception {
        Wrangler wrangler = new Wrangler(new Wrangler.Config("sql", "Yes",
                "true", "uppercase :Name", null,
                null, null, null, null));
        Relation relation = mock(Relation.class);
        Relation resultRelation = mock(Relation.class);
        resultRelation = wrangler.transform(null, relation);
    }

    @Test
    public void invalidRelationTest () {
        Wrangler wrangler = new Wrangler(new Wrangler.Config("jexl", "No",
                "false", "uppercase :Name", null,
                null, null, null, null));
        Relation relation = mock(Relation.class);
        Relation invalidrelation = new InvalidRelation("Plugin is not con" +
                "figured for relational transformation");
        Assert.assertEquals(invalidrelation.getValidationError(),
                wrangler.transform(null, relation).getValidationError());
    }
    
}
