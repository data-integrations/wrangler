/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 */

 package io.cdap.wrangler.registry;

 import com.google.common.annotations.VisibleForTesting;
 import io.cdap.cdap.api.artifact.ArtifactSummary;
 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.DirectiveLoadException;
 import org.reflections.Reflections;
 
 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import java.util.Set;
 import javax.annotation.Nullable;
 
 /**
  * This class is implementation of {@link DirectiveRegistry} for maintaining a registry
  * of system provided directives.
  */
 public final class SystemDirectiveRegistry implements DirectiveRegistry {
 
   public static final SystemDirectiveRegistry INSTANCE;
 
   static {
     try {
       INSTANCE = new SystemDirectiveRegistry();
     } catch (DirectiveLoadException e) {
       // This shouldn't happen
       throw new RuntimeException("Failed to load system directives", e);
     }
   }
 
   // This is the default package in which the directives are searched for.
   private static final String PACKAGE = "io.cdap.directives";
   private final Map<String, DirectiveInfo> registry;
 
   @VisibleForTesting
   SystemDirectiveRegistry() throws DirectiveLoadException {
     this(new ArrayList<>());
   }
 
   public SystemDirectiveRegistry(List<String> namespaces) throws DirectiveLoadException {
     Map<String, DirectiveInfo> registry = new HashMap<>();
 
     // 🔥 Add custom directive package for AggregateStats
     namespaces.add("io.cdap.directives.aggregates");
     namespaces.add(PACKAGE);
 
     for (String namespace : namespaces) {
       try {
         Reflections reflections = new Reflections(namespace);
         Set<Class<? extends Directive>> system = reflections.getSubTypesOf(Directive.class);
         for (Class<? extends Directive> directive : system) {
           DirectiveInfo info = DirectiveInfo.fromSystem(directive);
           registry.put(info.name(), info);
         }
       } catch (InstantiationException | IllegalAccessException e) {
         throw new DirectiveLoadException(e.getMessage(), e);
       }
     }
 
     this.registry = Collections.unmodifiableMap(registry);
   }
 
   @Override
   public DirectiveInfo get(String namespace, String name) {
     return get(name);
   }
 
   public DirectiveInfo get(String name) {
     return registry.get(name);
   }
 
   @Override
   public void reload(String namespace) {
     // No-op.
   }
 
   @Nullable
   @Override
   public ArtifactSummary getLatestWranglerArtifact() {
     return null;
   }
 
   @Override
   public Iterable<DirectiveInfo> list(String namespace) {
     return Collections.unmodifiableCollection(registry.values());
   }
 
   @Override
   public void close() {
     // No-op.
   }
 }
 