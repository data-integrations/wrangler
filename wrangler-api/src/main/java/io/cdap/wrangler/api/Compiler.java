/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.annotations.PublicEvolving;
import org.apache.twill.filesystem.Location;

import java.nio.file.Path;

/**
 * Interface for implementing directive or recipe compilers.
 * 
 * <p>Provides methods to compile recipes from different source formats including
 * strings, HDFS locations, and filesystem paths. The compiler processes the recipe
 * and produces executable directives.</p>
 *
 * <p>Compilation results are returned as {@link CompileStatus} objects containing either:
 * <ul>
 *   <li>Successfully compiled directives as {@link Executor} instances</li>
 *   <li>Compilation errors as an iterator of {@link io.cdap.wrangler.api.parser.SyntaxError}</li>
 * </ul>
 * </p>
 */
@PublicEvolving
public interface Compiler {
  /**
   * Compiles a recipe from a string.
   *
   * @param recipe The recipe contents to compile
   * @return CompileStatus containing either compiled directives or syntax errors
   * @throws CompileException if compilation fails due to system errors
   */
  CompileStatus compile(String recipe) throws CompileException;

  /**
   * Compiles a recipe from an HDFS location.
   *
   * @param location HDFS location containing the recipe to compile
   * @return CompileStatus containing either compiled directives or syntax errors
   * @throws CompileException if compilation fails due to IO or system errors
   */
  CompileStatus compile(Location location) throws CompileException;

  /**
   * Compiles a recipe from a filesystem path.
   *
   * @param path Path to the recipe file to compile
   * @return CompileStatus containing either compiled directives or syntax errors
   * @throws CompileException if compilation fails due to IO or system errors 
   */
  CompileStatus compile(Path path) throws CompileException;
}
