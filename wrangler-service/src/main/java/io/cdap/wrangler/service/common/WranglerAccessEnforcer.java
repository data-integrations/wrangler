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
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.wrangler.proto.id.DirectiveConfigEntityId;
import io.cdap.wrangler.proto.id.RecipeEntityId;
import io.cdap.wrangler.proto.id.WorkspaceEntityId;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Helper for enforcing RBAC permissions across Wrangler HTTP service handlers.
 *
 * <p>Workspace, recipe and directive config checks are each gated by their own feature flag.
 * Connection, instance, namespace and artifact checks outside workspace-gated directive endpoints
 * are always enforced.
 *
 * <p>{@link StandardPermission#CREATE} and {@link StandardPermission#LIST} are checked on the parent
 * namespace for the child type of the given entity, so the wildcard ({@code *}) overloads are used for
 * operations that are not tied to one existing entity, such as creating or listing.
 */
public class WranglerAccessEnforcer {
  private static final String ALL_ENTITIES = "*";

  private final ContextAccessEnforcer contextAccessEnforcer;
  private final boolean isWorkspaceAuthEnforcementEnabled;
  private final boolean isRecipeAuthEnforcementEnabled;
  private final boolean isDirectiveConfigAuthEnforcementEnabled;

  public WranglerAccessEnforcer(SystemHttpServiceContext context) {
    this.contextAccessEnforcer = Objects.requireNonNull(context.getContextAccessEnforcer(),
                                                        "ContextAccessEnforcer must not be null");
    this.isWorkspaceAuthEnforcementEnabled = Feature.WRANGLER_WORKSPACE_AUTH_CHECK.isEnabled(context);
    this.isRecipeAuthEnforcementEnabled = Feature.WRANGLER_RECIPE_AUTH_CHECK.isEnabled(context);
    this.isDirectiveConfigAuthEnforcementEnabled = Feature.WRANGLER_DIRECTIVE_CONFIG_AUTH_CHECK.isEnabled(context);
  }

  /**
   * Enforces {@code permission} on the wildcard workspace ({@code *}) in {@code namespace}, for
   * operations that are not tied to one existing workspace, such as creating or listing workspaces.
   * No-op unless {@link Feature#WRANGLER_WORKSPACE_AUTH_CHECK} is enabled.
   */
  public void enforceWorkspace(String namespace, StandardPermission permission) {
    enforceWorkspace(namespace, ALL_ENTITIES, permission);
  }

  /**
   * Enforces {@code permission} on a workspace.
   * No-op unless {@link Feature#WRANGLER_WORKSPACE_AUTH_CHECK} is enabled.
   */
  public void enforceWorkspace(String namespace, String workspaceId, StandardPermission permission) {
    if (isWorkspaceAuthEnforcementEnabled) {
      contextAccessEnforcer.enforce(new WorkspaceEntityId(namespace, workspaceId), permission);
    }
  }

  /**
   * Enforces permission to write a workspace whose ID may already exist, such as when the ID is derived
   * from an uploaded file name. Requires {@code CREATE}, and also {@code UPDATE} when {@code exists}
   * reports that the write overwrites an existing workspace. {@code exists} is only evaluated after the
   * {@code CREATE} check passes.
   * No-op unless {@link Feature#WRANGLER_WORKSPACE_AUTH_CHECK} is enabled.
   */
  public void enforceWorkspaceUpsert(String namespace, String workspaceId, BooleanSupplier exists) {
    if (!isWorkspaceAuthEnforcementEnabled) {
      return;
    }
    WorkspaceEntityId entityId = new WorkspaceEntityId(namespace, workspaceId);
    contextAccessEnforcer.enforce(entityId, StandardPermission.CREATE);
    if (exists.getAsBoolean()) {
      contextAccessEnforcer.enforce(entityId, StandardPermission.UPDATE);
    }
  }

  /**
   * Enforces {@code UPDATE} on {@code workspaceId} when {@code exists} reports that the workspace already
   * exists, for endpoints that already enforced {@code CREATE} up front. {@code exists} is only evaluated
   * when {@link Feature#WRANGLER_WORKSPACE_AUTH_CHECK} is enabled.
   */
  public void enforceWorkspaceUpdateIfExists(String namespace, String workspaceId, BooleanSupplier exists) {
    if (isWorkspaceAuthEnforcementEnabled && exists.getAsBoolean()) {
      contextAccessEnforcer.enforce(new WorkspaceEntityId(namespace, workspaceId), StandardPermission.UPDATE);
    }
  }

  /**
   * Enforces {@code permission} on {@code namespace} when {@link Feature#WRANGLER_WORKSPACE_AUTH_CHECK}
   * is enabled, preserving the existing flag scope on legacy v1 directive metadata endpoints.
   */
  public void enforceNamespaceForWorkspace(String namespace, StandardPermission permission) {
    if (isWorkspaceAuthEnforcementEnabled) {
      enforceNamespace(namespace, permission);
    }
  }

  /**
   * Enforces {@code permission} on {@code namespace} for its artifact children when
   * {@link Feature#WRANGLER_WORKSPACE_AUTH_CHECK} is enabled, preserving the existing flag scope on
   * legacy v1 directive artifact endpoints.
   */
  public void enforceArtifactOnParentForWorkspace(String namespace, StandardPermission permission) {
    if (isWorkspaceAuthEnforcementEnabled) {
      enforceArtifactOnParent(namespace, permission);
    }
  }

  /**
   * Enforces {@code permission} on the wildcard recipe ({@code *}) in {@code namespace}, for
   * operations that are not tied to one existing recipe, such as listing recipes.
   * No-op unless {@link Feature#WRANGLER_RECIPE_AUTH_CHECK} is enabled.
   */
  public void enforceRecipe(String namespace, StandardPermission permission) {
    enforceRecipe(namespace, ALL_ENTITIES, permission);
  }

  /**
   * Enforces {@code permission} on a recipe.
   * No-op unless {@link Feature#WRANGLER_RECIPE_AUTH_CHECK} is enabled.
   */
  public void enforceRecipe(String namespace, String recipeId, StandardPermission permission) {
    if (isRecipeAuthEnforcementEnabled) {
      contextAccessEnforcer.enforce(new RecipeEntityId(namespace, recipeId), permission);
    }
  }

  /**
   * Enforces {@code permission} on the system-wide directive config.
   * No-op unless {@link Feature#WRANGLER_DIRECTIVE_CONFIG_AUTH_CHECK} is enabled.
   */
  public void enforceDirectiveConfig(StandardPermission permission) {
    if (isDirectiveConfigAuthEnforcementEnabled) {
      contextAccessEnforcer.enforce(new DirectiveConfigEntityId(), permission);
    }
  }

  /**
   * Enforces {@code permission} on the wildcard connection ({@code *}) in {@code namespace}, for
   * operations that are not tied to one existing connection, such as creating, testing or listing
   * connections. Always enforced.
   */
  public void enforceConnection(String namespace, StandardPermission permission) {
    enforceConnection(namespace, ALL_ENTITIES, permission);
  }

  /**
   * Enforces {@code permission} on a connection. Always enforced.
   */
  public void enforceConnection(String namespace, String connectionId, StandardPermission permission) {
    contextAccessEnforcer.enforce(new ConnectionEntityId(namespace, connectionId), permission);
  }

  /**
   * Enforces {@code permission} on {@code namespace} for its artifact children, such as when listing
   * them. Always enforced.
   */
  public void enforceArtifactOnParent(String namespace, StandardPermission permission) {
    contextAccessEnforcer.enforceOnParent(EntityType.ARTIFACT, new NamespaceId(namespace), permission);
  }

  /**
   * Enforces {@code permission} on a namespace. Always enforced.
   */
  public void enforceNamespace(String namespace, StandardPermission permission) {
    contextAccessEnforcer.enforce(new NamespaceId(namespace), permission);
  }

  /**
   * Enforces {@code permission} on the CDAP instance ({@link InstanceId#SELF}). Always enforced.
   */
  public void enforceInstance(StandardPermission permission) {
    contextAccessEnforcer.enforce(InstanceId.SELF, permission);
  }
}
