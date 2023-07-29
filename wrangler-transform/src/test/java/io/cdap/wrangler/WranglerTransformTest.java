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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExpressionFactoryType;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;

import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.directives.transformation.Lower;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.utils.SqlExpressionGenerator;
import org.apache.poi.ss.formula.functions.T;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
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

    public static Relation runTransform(String[] recipe,
                                        RelationalTranformContext relationalTranformContext,
                                        Relation relation)
            throws DirectiveParseException, RecipeException {
        DirectiveRegistry registry;
        registry = SystemDirectiveRegistry.INSTANCE;
        try {
            registry.reload("default");
        } catch (DirectiveLoadException e) {
            throw new RuntimeException(e);
        }

        GrammarBasedParser parser = new GrammarBasedParser("default",
                new MigrateToV2(recipe).migrate(), registry);
        List<Directive> directives = parser.parse();
        for (Directive directive : directives) {
            relation = directive.transform(relationalTranformContext, relation);
        }
        return relation;
    }
}
