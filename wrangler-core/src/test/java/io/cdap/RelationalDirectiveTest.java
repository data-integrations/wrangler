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

package io.cdap;

import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import java.util.List;

public class RelationalDirectiveTest {

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
