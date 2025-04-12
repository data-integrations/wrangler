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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Tests for {@link AggregateStatsDirective} directive registration.
 */
public class AggregateStatsRegistrationTest {

  @Test
  public void testDirectiveRegistration() {
    // Get the system directive registry
    SystemDirectiveRegistry registry = SystemDirectiveRegistry.INSTANCE;
    
    // Get all registered directives
    Iterable<DirectiveInfo> directives = registry.list("aggregate");
    
    // Convert to list for easier processing
    List<DirectiveInfo> directiveList = StreamSupport.stream(directives.spliterator(), false)
        .collect(Collectors.toList());
    
    // Find the aggregate-stats directive
    DirectiveInfo aggregateStatsInfo = directiveList.stream()
        .filter(info -> "aggregate-stats".equals(info.name()))
        .findFirst()
        .orElse(null);
    
    // Verify the directive is registered
    Assert.assertNotNull("aggregate-stats directive should be registered", aggregateStatsInfo);
    
    // Verify the directive name
    Assert.assertEquals("aggregate-stats", aggregateStatsInfo.name());
    
    // Verify the directive class
    Assert.assertEquals(AggregateStatsDirective.class.getName(), 
                       aggregateStatsInfo.getDirectiveClass().getClassName());
  }
} 
