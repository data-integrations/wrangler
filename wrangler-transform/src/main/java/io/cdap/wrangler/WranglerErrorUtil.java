/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveNotFoundException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.utils.RecordConvertorException;
import java.util.List;
import java.util.Map;

/**
 * Error util file to handle exceptions caught in Wrangler plugin
 */
public final class WranglerErrorUtil {


  private static final Map<String, String> TERMINAL_EXCEPTIONS = ImmutableMap.<String, String>builder()
      .put(DirectiveParseException.class.getName(), "Parsing-Directive")
      .put(PreconditionException.class.getName(), "Precondition")
      .put(DirectiveExecutionException.class.getName(), "Executing-Directive")
      .put(DirectiveLoadException.class.getName(), "Loading-Directive")
      .put(DirectiveNotFoundException.class.getName(), "Directive-Not-Found")
      .put(RecordConvertorException.class.getName(), "Record-Conversion")
      .put(ELException.class.getName(), "ExpressionLanguage-Parsing").build();

  private static final Map<String, String> NON_TERMINAL_EXCEPTIONS = ImmutableMap.<String, String>builder()
      .put(RecipeException.class.getName(), "Executing-Recipe").build();

  /**
   * Private constructor to prevent instantiation of this utility class.
   * <p>
   * This class is designed to contain only static utility methods for handling exceptions and
   * should not be instantiated. Any attempt to create an instance of this class will result in an
   * {@link IllegalStateException}.
   */
  private WranglerErrorUtil() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Traverses the causal chain of the given Throwable to find specific exceptions. If a terminal
   * exception is found, it returns a corresponding ProgramFailureException. If a non-terminal
   * exception is found, it is stored as a fallback. Otherwise, a generic ProgramFailureException is
   * returned.
   *
   * @param e            the Throwable to analyze
   * @param errorReason  the error reason to tell the cause of error
   * @param errorMessage default error message if no terminal exception is found
   * @param errorType    the error type to categorize the failure
   * @return a ProgramFailureException with specific or generic error details
   */
  public static ProgramFailureException getProgramFailureExceptionDetailsFromChain(Throwable e,
      String errorReason, String errorMessage, ErrorType errorType) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    Throwable nonTerminalException = null;
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        return null; // Avoid multiple wrap
      }
      if (NON_TERMINAL_EXCEPTIONS.containsKey(t.getClass().getName())) {
        nonTerminalException = t; // Store non-terminal exception as fallback
        continue;
      }
      String errorSubCategory = TERMINAL_EXCEPTIONS.get(t.getClass().getName());
      if (errorSubCategory != null) {
        return getProgramFailureException(t, errorReason, errorSubCategory);
      }
    }

    if (nonTerminalException != null) {
      return getProgramFailureException(nonTerminalException, errorReason,
          NON_TERMINAL_EXCEPTIONS.get(nonTerminalException.getClass().getName()));
    }

    return ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason, errorMessage,
        errorType, false, e);
  }

  /**
   * Constructs a ProgramFailureException using the provided exception details.
   *
   * @param exception        the exception to wrap
   * @param errorSubCategory specific subcategory of the error
   * @return a new ProgramFailureException with the extracted details
   */
  private static ProgramFailureException getProgramFailureException(Throwable exception,
      String errorReason, String errorSubCategory) {
    String errorMessage = exception.getMessage();
    return ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN, errorSubCategory), errorReason,
        errorMessage, ErrorType.USER, false, exception);
  }
}
