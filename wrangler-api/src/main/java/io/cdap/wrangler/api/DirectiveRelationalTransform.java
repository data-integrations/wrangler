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

package io.cdap.wrangler.api;

import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.LinearRelationalTransform;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.RelationalTransform;

/**
 * {@link DirectiveRelationalTransform} provides relational transform support for
 * wrangler directives.
 */
public interface DirectiveRelationalTransform extends LinearRelationalTransform {

    /**
     * Implementation of linear relational transform for each supported directive.
     *
     * @param relationalTranformContext transformation context with engine, input and output parameters
     * @param relation input relation upon which the transformation is applied.
     * @return transformed relation as the output relation. By default, returns an Invalid relation
     * for unsupported directives.
     */
    default Relation transform(RelationalTranformContext relationalTranformContext,
                               Relation relation) {
    return new InvalidRelation("SQL execution for the directive is currently not supported.");
    }

    /**
     * Indicates whether the directive is supported by relational transformation or not.
     *
     * @return boolean value for the directive SQL support.
     * By default, returns false, indicating that the directive is currently not supported.
     */
    default boolean isSQLSupported() {
        return false;
    }

}
