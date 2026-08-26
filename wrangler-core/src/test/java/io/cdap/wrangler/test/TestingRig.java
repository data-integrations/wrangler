/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.wrangler.test;

import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import org.mockito.Mockito;

import java.util.List;

/**
 * A testing utility for executing directives on rows.
 */
public class TestingRig {
    private final DirectiveRegistry registry;
    private final ExecutorContext context;

    public TestingRig() {
        try {
            this.registry = new SystemDirectiveRegistry();
            this.context = Mockito.mock(ExecutorContext.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes the given directive on the input rows.
     *
     * @param rows List of input rows
     * @param directive Directive to execute
     * @return List of transformed rows
     * @throws DirectiveParseException if parsing fails
     * @throws DirectiveExecutionException if execution fails
     */
    public List<Row> execute(List<Row> rows, String directive) 
        throws DirectiveParseException, DirectiveExecutionException {
        return registry.get(directive).execute(rows, context);
    }
} 