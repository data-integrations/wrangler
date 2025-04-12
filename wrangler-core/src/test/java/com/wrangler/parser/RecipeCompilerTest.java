/*
 * Copyright 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import io.cdap.wrangler.parser.RecipeCompiler;
import org.junit.Test;
import static org.junit.Assert.assertFalse;

public class RecipeCompilerTest {

    @Test
    public void testInvalidRecipe() {
        String invalidRecipe = "processRecipe(\"inputFile\", \"1X\", \"20s\")";  // Invalid Byte Size
        RecipeCompiler compiler = new RecipeCompiler();
        boolean isValid = compiler.compile(invalidRecipe);  // Should fail parsing
        assertFalse(isValid);
    }

    @Test
    public void testInvalidTimeDuration() {
        String invalidRecipe = "processRecipe(\"inputFile\", \"10MB\", \"invalidDuration\")";  // Invalid Time Duration
        RecipeCompiler compiler = new RecipeCompiler();
        boolean isValid = compiler.compile(invalidRecipe);  // Should fail parsing
        assertFalse(isValid);
    }
}
