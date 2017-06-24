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

package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;

/**
 * Class description here.
 */
@PublicEvolving
public enum TokenType implements Serializable {
  DIRECTIVE_NAME,
  COLUMN_NAME,
  TEXT,
  NUMERIC,
  BOOLEAN,
  COLUMN_NAME_LIST,
  TEXT_LIST,
  NUMERIC_LIST,
  BOOLEAN_LIST,
  EXPRESSION,
  UUD_DIRECTIVE
}
