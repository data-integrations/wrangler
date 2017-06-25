/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.parser;

import co.cask.wrangler.api.annotations.PublicEvolving;
import co.cask.wrangler.api.parser.SyntaxError;
import org.apache.twill.filesystem.Location;

import java.nio.file.Path;
import java.util.Iterator;

/**
 * Class description here.
 */
@PublicEvolving
public interface Compiler {
  Iterator<CompiledUnit> compile(Iterator<String> directive) throws CompileException;
  CompiledUnit compile(String directive) throws CompileException;
  CompiledUnit compile(Location location) throws CompileException;
  CompiledUnit compile(Path path) throws CompileException;
  boolean hasErrors();
  Iterator<SyntaxError> getSyntaxErrors();
}
