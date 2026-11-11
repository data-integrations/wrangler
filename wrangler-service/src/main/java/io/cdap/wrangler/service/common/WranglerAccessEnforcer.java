/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.wrangler.service.common;

import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ConnectionEntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.wrangler.proto.id.DirectiveConfigEntityId;
import io.cdap.wrangler.proto.id.RecipeEntityId;
import io.cdap.wrangler.proto.id.WorkspaceEntityId;

/**
 * Helper for enforcing RBAC permissions across Wrangler HTTP service handlers.
 */
public class WranglerAccessEnforcer {
  public static final String ALL_ENTITIES = "*";

  private final ContextAccessEnforcer contextAccessEnforcer;
  private final boolean isWorkspaceAuthEnforcementEnabled;
  private final boolean isRecipeAuthEnforcementEnabled;
  private final boolean isDirectiveConfigAuthEnforcementEnabled;

  public WranglerAccessEnforcer(SystemHttpServiceContext context) {
    this.contextAccessEnforcer = context.getContextAccessEnforcer();
    this.isWorkspaceAuthEnforcementEnabled = Feature.WRANGLER_WORKSPACE_AUTH_CHECK.isEnabled(context);
    this.isRecipeAuthEnforcementEnabled = Feature.WRANGLER_RECIPE_AUTH_CHECK.isEnabled(context);
    this.isDirectiveConfigAuthEnforcementEnabled = Feature.WRANGLER_DIRECTIVE_CONFIG_AUTH_CHECK.isEnabled(context);
  }

  public void enforceWorkspace(String namespace, StandardPermission permission) {
    enforceWorkspace(namespace, ALL_ENTITIES, permission);
  }

  public void enforceWorkspace(String namespace, String workspaceId, StandardPermission permission) {
    if (isWorkspaceAuthEnforcementEnabled && contextAccessEnforcer != null) {
      contextAccessEnforcer.enforce(new WorkspaceEntityId(namespace, workspaceId), permission);
    }
  }

  public void enforceOnParentNamespace(String namespace, StandardPermission permission) {
    if (isWorkspaceAuthEnforcementEnabled && contextAccessEnforcer != null) {
      contextAccessEnforcer.enforceOnParent(EntityType.SYSTEM_APP_ENTITY, new NamespaceId(namespace), permission);
    }
  }

  public void enforceRecipe(String namespace, String recipeId, StandardPermission permission) {
    if (isRecipeAuthEnforcementEnabled && contextAccessEnforcer != null) {
      contextAccessEnforcer.enforce(new RecipeEntityId(namespace, recipeId), permission);
    }
  }

  public void enforceOnParentNamespaceForRecipe(String namespace, StandardPermission permission) {
    if (isRecipeAuthEnforcementEnabled && contextAccessEnforcer != null) {
      contextAccessEnforcer.enforceOnParent(EntityType.SYSTEM_APP_ENTITY, new NamespaceId(namespace), permission);
    }
  }

  public void enforceDirectiveConfig(StandardPermission permission) {
    if (isDirectiveConfigAuthEnforcementEnabled && contextAccessEnforcer != null) {
      contextAccessEnforcer.enforce(new DirectiveConfigEntityId(), permission);
    }
  }

  public void enforceConnection(String namespace, StandardPermission permission) {
    enforceConnection(namespace, ALL_ENTITIES, permission);
  }

  public void enforceConnection(String namespace, String connectionId, StandardPermission permission) {
    if (contextAccessEnforcer != null) {
      contextAccessEnforcer.enforce(new ConnectionEntityId(namespace, connectionId), permission);
    }
  }

  public void enforceNamespace(String namespace, StandardPermission permission) {
    if (contextAccessEnforcer != null) {
      contextAccessEnforcer.enforce(new NamespaceId(namespace), permission);
    }
  }
}
