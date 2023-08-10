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

import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MockEngine implements Engine {

    @Override
    public Set<Capability> getCapabilities() {
        return Collections.singleton(StringExpressionFactoryType.SQL);
    }

    @Override
    public List<ExpressionFactory<?>> getExpressionFactories() {
    return Collections.singletonList(new MockExpressionFactory());
    }
}
