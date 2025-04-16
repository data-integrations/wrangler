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
 * This <code>Compiler</code> interface provides a way to implement your
 * own version of compiler for directive or recipe.
 *
 * <p>This interface contains methods that provides variants of the source
 * from which the recipe are read. It support reading from string, HDFS location
 * and <code>Path</code>.</p>
 *
 * <p>Each of the methods would return <code>CompileStatus</code> objects that
 * contains the compiled directives in the form of <code>Executor</code> or
 * iterator of <code>SyntaxError</code> in case of failure to compile.</p>
 */
@PublicEvolving
public interface Compiler {
  /**
   * Compiles the recipe that is supplied in a <code>String</code> format.
   *
   * @param recipe representing the <code>String</code> form of recipe.
   * @return <code>CompileStatus</code> status of compilation.
   */
  CompileStatus compile(String recipe) throws CompileException;

  /**
   * Compiles the recipe that is supplied in a <code>Location</code> on HDFS.
   *
   * @param location Location to the recipe being compiled.
   * @return <code>CompileStatus</code> status of compilation.
   */
  CompileStatus compile(Location location) throws CompileException;

  /**
   * Compiles the recipe that is supplied in a <code>Path</code> on Filesystem.
   *
   * @param path <code>Path</code> to the recipe being compiled.
   * @return <code>CompileStatus</code> status of compilation.
   */
  CompileStatus compile(Path path) throws CompileException;
}
