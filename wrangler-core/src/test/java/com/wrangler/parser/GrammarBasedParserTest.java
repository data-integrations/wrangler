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
import static org.junit.Assert.assertTrue;

public class GrammarBasedParserTest {

    @Test
    public void testValidByteSizeAndTimeDuration() {
        String recipe = "processRecipe(\"inputFile\", \"10MB\", \"5s\")";
        RecipeCompiler compiler = new RecipeCompiler();
        boolean isValid = compiler.compile(recipe);  // Assuming compile() checks the recipe
        assertTrue(isValid);
    }

    @Test
    public void testInvalidByteSizeAndTimeDuration() {
        String invalidRecipe = "processRecipe(\"inputFile\", \"10unknown\", \"5s\")";
        RecipeCompiler compiler = new RecipeCompiler();
        boolean isValid = compiler.compile(invalidRecipe);  // Should fail
        assertFalse(isValid);
    }
}
