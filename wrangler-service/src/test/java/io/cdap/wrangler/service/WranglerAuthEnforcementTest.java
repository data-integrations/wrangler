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

package io.cdap.wrangler.service;

import com.google.gson.Gson;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ConnectionEntityId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldValidator;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.dataset.workspace.ConfigStore;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.StatusCodeException;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.id.DirectiveConfigEntityId;
import io.cdap.wrangler.proto.id.RecipeEntityId;
import io.cdap.wrangler.proto.id.WorkspaceEntityId;
import io.cdap.wrangler.proto.recipe.v2.Recipe;
import io.cdap.wrangler.service.adls.ADLSHandler;
import io.cdap.wrangler.service.bigquery.BigQueryHandler;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.common.WranglerAccessEnforcer;
import io.cdap.wrangler.service.connections.ConnectionHandler;
import io.cdap.wrangler.service.connections.ConnectionTypeConfig;
import io.cdap.wrangler.service.database.DatabaseHandler;
import io.cdap.wrangler.service.directive.ConnectionDiscoverer;
import io.cdap.wrangler.service.directive.DirectivesHandler;
import io.cdap.wrangler.service.directive.RecipeHandler;
import io.cdap.wrangler.service.directive.WorkspaceHandler;
import io.cdap.wrangler.service.explorer.FilesystemExplorer;
import io.cdap.wrangler.service.gcs.GCSHandler;
import io.cdap.wrangler.service.kafka.KafkaHandler;
import io.cdap.wrangler.service.s3.S3Handler;
import io.cdap.wrangler.service.schema.DataModelHandler;
import io.cdap.wrangler.service.schema.SchemaRegistryHandler;
import io.cdap.wrangler.service.spanner.SpannerHandler;
import io.cdap.wrangler.store.recipe.RecipeStore;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for RBAC authorization enforcement and system namespace validation across Wrangler HTTP handlers.
 */
public class WranglerAuthEnforcementTest {
  private static final Gson GSON = new Gson();
  private static final String TEST_NS = "default";
  private static final String MISSING_NS = "missing_ns";

  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setupTables() {
    Map<StructuredTableId, StructuredTable> tables = new HashMap<>();
    for (StructuredTableSpecification spec : Arrays.asList(
      ConnectionStore.TABLE_SPEC, WorkspaceDataset.TABLE_SPEC, ConfigStore.TABLE_SPEC, RecipeStore.RECIPE_TABLE_SPEC)) {
      tables.put(spec.getTableId(), createInMemoryTable(spec));
    }
    StructuredTableContext tableContext = tables::get;
    transactionRunner = runnable -> {
      try {
        runnable.run(tableContext);
      } catch (Exception e) {
        throw new TransactionException("Transaction failed", e);
      }
    };
  }

  @Test
  public void testAbstractWranglerHandlerRespondUnauthorizedAndSystemNamespace() throws Exception {
    RecordingAccessEnforcer enforcer = new RecordingAccessEnforcer(true);
    TestWranglerHandler handler = new TestWranglerHandler();
    handler.initialize(createMockContext(enforcer, true));

    HttpServiceRequest request = createMockRequest("{}");

    // 1. Overload 1: respond(request, responder, callable) returns 403 on UnauthorizedException
    AtomicInteger status1 = new AtomicInteger();
    handler.callRespondNoNamespace(request, createMockResponder(status1), () -> {
      enforcer.enforce(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE);
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, status1.get());

    // 2. Overload 2: respond(responder, namespace, consumer) returns 403 on UnauthorizedException
    AtomicInteger status2 = new AtomicInteger();
    handler.callRespondConsumer(createMockResponder(status2), TEST_NS, () -> {
      enforcer.enforce(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET);
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, status2.get());

    // 3. Overload 2: respond(responder, "system", consumer) returns 400
    AtomicInteger status2Sys = new AtomicInteger();
    handler.callRespondConsumer(createMockResponder(status2Sys), "system", () -> {
      Assert.fail("System namespace should be rejected before consumer executes");
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, status2Sys.get());

    // 4. Overload 3: respond(request, responder, namespace, function) returns 403 on UnauthorizedException
    AtomicInteger status3 = new AtomicInteger();
    handler.callRespondFunction(request, createMockResponder(status3), TEST_NS, () -> {
      enforcer.enforce(new ConnectionEntityId(TEST_NS, "conn1"), StandardPermission.USE);
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, status3.get());

    // 5. Overload 3: respond(request, responder, "system", function) returns 400 (case-insensitive)
    AtomicInteger status3Sys = new AtomicInteger();
    handler.callRespondFunction(request, createMockResponder(status3Sys), "SYSTEM", () -> {
      Assert.fail("System namespace should be rejected before function executes");
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, status3Sys.get());

    // 6. Missing namespace returns 404 across both namespaced respond overloads
    AtomicInteger status2Missing = new AtomicInteger();
    handler.callRespondConsumer(createMockResponder(status2Missing), MISSING_NS, () -> {
      Assert.fail("Missing namespace should return 404 before consumer executes");
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, status2Missing.get());

    AtomicInteger status3Missing = new AtomicInteger();
    handler.callRespondFunction(request, createMockResponder(status3Missing), MISSING_NS, () -> {
      Assert.fail("Missing namespace should return 404 before function executes");
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, status3Missing.get());

    // 7. Allow paths return 200 OK across all respond overloads
    RecordingAccessEnforcer allowEnforcer = new RecordingAccessEnforcer(false);
    TestWranglerHandler allowHandler = new TestWranglerHandler();
    allowHandler.initialize(createMockContext(allowEnforcer, true));

    AtomicInteger allowStatus1 = new AtomicInteger();
    allowHandler.callRespondNoNamespace(request, createMockResponder(allowStatus1), () -> {
      allowEnforcer.enforce(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE);
      return Collections.singletonMap("ok", true);
    });
    Assert.assertEquals(HttpURLConnection.HTTP_OK, allowStatus1.get());

    AtomicInteger allowStatus2 = new AtomicInteger();
    HttpServiceResponder allowResponder2 = createMockResponder(allowStatus2);
    allowHandler.callRespondConsumer(allowResponder2, TEST_NS, () -> {
      allowEnforcer.enforce(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET);
      allowResponder2.sendStatus(HttpURLConnection.HTTP_OK);
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_OK, allowStatus2.get());
    AtomicInteger allowStatus3 = new AtomicInteger();
    allowHandler.callRespondFunction(request, createMockResponder(allowStatus3), TEST_NS, () -> {
      allowEnforcer.enforce(new ConnectionEntityId(TEST_NS, "conn1"), StandardPermission.USE);
      return Collections.singletonMap("ok", true);
    });
    Assert.assertEquals(HttpURLConnection.HTTP_OK, allowStatus3.get());
    Assert.assertFalse(allowHandler.checkShouldCopyHeader(null));
    Assert.assertFalse(allowHandler.checkShouldCopyHeader(new NamespacedId(new Namespace(TEST_NS, 1L), null)));
  }

  @Test
  public void testWranglerAccessEnforcerFeatureFlagsDisabledAndNullEnforcer() {
    try {
      new WranglerAccessEnforcer(createMockContext(null, true));
      Assert.fail("Expected NullPointerException when ContextAccessEnforcer is null");
    } catch (NullPointerException expected) {
      // expected fail-closed check
    }

    RecordingAccessEnforcer allowEnforcer = new RecordingAccessEnforcer(false);
    WranglerAccessEnforcer accessEnforcer = new WranglerAccessEnforcer(createMockContext(allowEnforcer, false));

    accessEnforcer.enforceWorkspace(TEST_NS, StandardPermission.CREATE);
    accessEnforcer.enforceWorkspace(TEST_NS, "ws1", StandardPermission.GET);
    accessEnforcer.enforceWorkspace(TEST_NS, StandardPermission.LIST);
    accessEnforcer.enforceWorkspaceUpsert(TEST_NS, "ws1", () -> {
      throw new AssertionError("Existence must not be checked when workspace auth is disabled");
    });
    accessEnforcer.enforceWorkspaceUpdateIfExists(TEST_NS, "ws1", () -> {
      throw new AssertionError("Existence must not be checked when workspace auth is disabled");
    });
    accessEnforcer.enforceNamespaceForWorkspace(TEST_NS, StandardPermission.GET);
    accessEnforcer.enforceArtifactOnParentForWorkspace(TEST_NS, StandardPermission.LIST);
    accessEnforcer.enforceRecipe(TEST_NS, "r1", StandardPermission.GET);
    accessEnforcer.enforceRecipe(TEST_NS, StandardPermission.LIST);
    accessEnforcer.enforceDirectiveConfig(StandardPermission.UPDATE);
    Assert.assertNull(allowEnforcer.getLastCall());

    accessEnforcer.enforceConnection(TEST_NS, StandardPermission.CREATE);
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        allowEnforcer.getLastCall());
    accessEnforcer.enforceNamespace(TEST_NS, StandardPermission.GET);
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        allowEnforcer.getLastCall());
    accessEnforcer.enforceArtifactOnParent(TEST_NS, StandardPermission.LIST);
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.LIST, EntityType.ARTIFACT),
                        allowEnforcer.getLastCall());
    accessEnforcer.enforceInstance(StandardPermission.UPDATE);
    Assert.assertEquals(new EnforceCall(InstanceId.SELF, StandardPermission.UPDATE),
                        allowEnforcer.getLastCall());

    // Per-feature flag isolation: enabling one flag does not enable the other two
    RecordingAccessEnforcer wsOnlyEnforcer = new RecordingAccessEnforcer(false);
    WranglerAccessEnforcer wsOnly = new WranglerAccessEnforcer(
      createMockContext(wsOnlyEnforcer, EnumSet.of(Feature.WRANGLER_WORKSPACE_AUTH_CHECK), Collections.emptyMap()));
    wsOnly.enforceWorkspace(TEST_NS, "ws1", StandardPermission.GET);
    wsOnly.enforceNamespaceForWorkspace(TEST_NS, StandardPermission.GET);
    wsOnly.enforceArtifactOnParentForWorkspace(TEST_NS, StandardPermission.LIST);
    wsOnly.enforceRecipe(TEST_NS, "r1", StandardPermission.GET);
    wsOnly.enforceDirectiveConfig(StandardPermission.GET);
    Assert.assertEquals(3, wsOnlyEnforcer.getCalls().size());

    RecordingAccessEnforcer recipeOnlyEnforcer = new RecordingAccessEnforcer(false);
    WranglerAccessEnforcer recipeOnly = new WranglerAccessEnforcer(
      createMockContext(recipeOnlyEnforcer, EnumSet.of(Feature.WRANGLER_RECIPE_AUTH_CHECK), Collections.emptyMap()));
    recipeOnly.enforceWorkspace(TEST_NS, "ws1", StandardPermission.GET);
    recipeOnly.enforceNamespaceForWorkspace(TEST_NS, StandardPermission.GET);
    recipeOnly.enforceRecipe(TEST_NS, "r1", StandardPermission.GET);
    recipeOnly.enforceDirectiveConfig(StandardPermission.GET);
    Assert.assertEquals(Collections.singletonList(new EnforceCall(new RecipeEntityId(TEST_NS, "r1"),
                                                                  StandardPermission.GET)),
                        recipeOnlyEnforcer.getCalls());

    RecordingAccessEnforcer configOnlyEnforcer = new RecordingAccessEnforcer(false);
    WranglerAccessEnforcer configOnly = new WranglerAccessEnforcer(
      createMockContext(configOnlyEnforcer, EnumSet.of(Feature.WRANGLER_DIRECTIVE_CONFIG_AUTH_CHECK),
                        Collections.emptyMap()));
    configOnly.enforceWorkspace(TEST_NS, "ws1", StandardPermission.GET);
    configOnly.enforceRecipe(TEST_NS, "r1", StandardPermission.GET);
    configOnly.enforceDirectiveConfig(StandardPermission.UPDATE);
    Assert.assertEquals(Collections.singletonList(new EnforceCall(new DirectiveConfigEntityId(),
                                                                  StandardPermission.UPDATE)),
                        configOnlyEnforcer.getCalls());
  }

  @Test
  public void testWranglerAccessEnforcerWorkspaceUpsert() {
    RecordingAccessEnforcer allowEnforcer = new RecordingAccessEnforcer(false);
    WranglerAccessEnforcer accessEnforcer = new WranglerAccessEnforcer(createMockContext(allowEnforcer, true));
    WorkspaceEntityId workspace = new WorkspaceEntityId(TEST_NS, "ws1");

    // New workspace requires only CREATE
    accessEnforcer.enforceWorkspaceUpsert(TEST_NS, "ws1", () -> false);
    Assert.assertEquals(Collections.singletonList(new EnforceCall(workspace, StandardPermission.CREATE)),
                        allowEnforcer.getCalls());

    // Overwriting an existing workspace requires CREATE and UPDATE
    allowEnforcer.clear();
    accessEnforcer.enforceWorkspaceUpsert(TEST_NS, "ws1", () -> true);
    Assert.assertEquals(Arrays.asList(new EnforceCall(workspace, StandardPermission.CREATE),
                                      new EnforceCall(workspace, StandardPermission.UPDATE)),
                        allowEnforcer.getCalls());

    // Existence is not checked when CREATE is denied
    RecordingAccessEnforcer denyEnforcer = new RecordingAccessEnforcer(true);
    WranglerAccessEnforcer denyingAccessEnforcer =
      new WranglerAccessEnforcer(createMockContext(denyEnforcer, true));
    try {
      denyingAccessEnforcer.enforceWorkspaceUpsert(TEST_NS, "ws1", () -> {
        throw new AssertionError("Existence must not be checked before CREATE is authorized");
      });
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException expected) {
      Assert.assertEquals(new EnforceCall(workspace, StandardPermission.CREATE), denyEnforcer.getLastCall());
    }
  }

  @Test
  public void testConnectionHandlerRbacAndPreconfiguredProtection() throws Exception {
    Connection preconfigured = new Connection(
      new NamespacedId(new Namespace(TEST_NS, 1L), "preconfigured_gcs"),
      ConnectionType.GCS, "Preconfigured GCS", "", 0L, 0L, Collections.emptyMap(), true);
    ConnectionTypeConfig config = new ConnectionTypeConfig(
      Collections.emptySet(), Collections.singletonList(preconfigured), null);

    // 1. Verify RBAC denial returns 403 across all ConnectionHandler endpoints
    RecordingAccessEnforcer denyEnforcer = new RecordingAccessEnforcer(true);
    ConnectionHandler handler = createConnectionHandler(config, denyEnforcer, true);
    HttpServiceRequest request = createMockRequest("{\"name\":\"c1\",\"type\":\"GCS\",\"properties\":{}}");

    assertEndpointForbidden(status -> handler.create(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());

    assertEndpointForbidden(status -> handler.list(request, createMockResponder(status), TEST_NS, "*"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.LIST),
                        denyEnforcer.getLastCall());

    assertEndpointForbidden(status -> handler.get(request, createMockResponder(status), TEST_NS, "c1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "c1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    assertEndpointForbidden(status -> handler.properties(request, createMockResponder(status), TEST_NS, "c1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "c1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    assertEndpointForbidden(status -> handler.clone(request, createMockResponder(status), TEST_NS, "c1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "c1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    assertEndpointForbidden(status -> handler.update(request, createMockResponder(status), TEST_NS, "c1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "c1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());

    assertEndpointForbidden(
      status -> handler.updateProp(request, createMockResponder(status), TEST_NS, "c1", "k", "v"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "c1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());

    assertEndpointForbidden(status -> handler.delete(request, createMockResponder(status), TEST_NS, "c1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "c1"), StandardPermission.DELETE),
                        denyEnforcer.getLastCall());

    // 2. Verify preconfigured connection in TEST_NS cannot be updated or deleted even when RBAC allows (401)
    RecordingAccessEnforcer allowEnforcer = new RecordingAccessEnforcer(false);
    ConnectionHandler allowHandler = createConnectionHandler(config, allowEnforcer, true);
    String preconfiguredId = "preconfigured_gcs";

    AtomicInteger updateStatus = new AtomicInteger();
    allowHandler.update(request, createMockResponder(updateStatus), TEST_NS, preconfiguredId);
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, updateStatus.get());

    AtomicInteger updatePropStatus = new AtomicInteger();
    allowHandler.updateProp(request, createMockResponder(updatePropStatus), TEST_NS, preconfiguredId, "k", "v");
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, updatePropStatus.get());

    AtomicInteger deleteStatus = new AtomicInteger();
    allowHandler.delete(request, createMockResponder(deleteStatus), TEST_NS, preconfiguredId);
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, deleteStatus.get());

    // 3. Verify user connection with the SAME name in another namespace can be created, updated, and deleted (200)
    HttpServiceRequest sameNameReq =
      createMockRequest("{\"name\":\"Preconfigured GCS\",\"type\":\"GCS\",\"properties\":{\"k\":\"v1\"}}");
    String otherNs = "team_b";
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.create(sameNameReq, createMockResponder(status), otherNs));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.update(sameNameReq, createMockResponder(status), otherNs,
                                                       preconfiguredId));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.updateProp(sameNameReq, createMockResponder(status), otherNs,
                                                           preconfiguredId, "k", "v2"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.delete(sameNameReq, createMockResponder(status), otherNs,
                                                       preconfiguredId));

    // 4. Verify allow-path CRUD on user connection in TEST_NS and idempotent delete on missing connection (200)
    HttpServiceRequest userConnReq =
      createMockRequest("{\"name\":\"User Conn\",\"type\":\"GCS\",\"properties\":{\"bucket\":\"b1\"}}");
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.create(userConnReq, createMockResponder(status), TEST_NS));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.list(userConnReq, createMockResponder(status), TEST_NS, "*"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.get(userConnReq, createMockResponder(status), TEST_NS, "user_conn"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.properties(userConnReq, createMockResponder(status), TEST_NS,
                                                           "user_conn"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.clone(userConnReq, createMockResponder(status), TEST_NS, "user_conn"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.update(userConnReq, createMockResponder(status), TEST_NS,
                                                       "user_conn"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.updateProp(userConnReq, createMockResponder(status), TEST_NS,
                                                           "user_conn", "bucket", "b2"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.delete(userConnReq, createMockResponder(status), TEST_NS,
                                                       "user_conn"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowHandler.delete(userConnReq, createMockResponder(status), TEST_NS,
                                                       "MixedCase_Missing"));
  }

  @Test
  public void testConnectorHandlersRbacEnforcement() throws Exception {
    RecordingAccessEnforcer denyEnforcer = new RecordingAccessEnforcer(true);
    SystemHttpServiceContext context = createMockContext(denyEnforcer, true);
    HttpServiceRequest request = createMockRequest("{}");

    // BigQueryHandler
    BigQueryHandler bqHandler = new BigQueryHandler();
    bqHandler.initialize(context);
    assertEndpointForbidden(status -> bqHandler.testBiqQueryConnection(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> bqHandler.listDatasets(request, createMockResponder(status), TEST_NS, "bq1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "bq1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> bqHandler.listTables(request, createMockResponder(status), TEST_NS, "bq1", "ds1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "bq1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> bqHandler.readTable(request, createMockResponder(status), TEST_NS, "bq1", "ds1", "t1", "default"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "bq1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> bqHandler.specification(request, createMockResponder(status), TEST_NS, "bq1", "ws1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "bq1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> bqHandler.specification(request, createMockResponder(status), TEST_NS, "bq1", null));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "bq1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());

    // When wid is null and connection USE is allowed, enforceWorkspace is skipped (no WorkspaceEntityId NPE)
    // and existing getWorkspace response behavior is preserved.
    RecordingAccessEnforcer allowBqEnforcer = new RecordingAccessEnforcer(false);
    BigQueryHandler allowBqHandler = new BigQueryHandler();
    allowBqHandler.initialize(createMockContext(allowBqEnforcer, true));
    assertEndpointStatus(HttpURLConnection.HTTP_INTERNAL_ERROR,
                         status -> allowBqHandler.specification(request, createMockResponder(status), TEST_NS,
                                                                "bq1", null));
    Assert.assertEquals(Collections.singletonList(new EnforceCall(new ConnectionEntityId(TEST_NS, "bq1"),
                                                                  StandardPermission.USE)),
                        allowBqEnforcer.getCalls());

    // GCSHandler
    GCSHandler gcsHandler = new GCSHandler();
    gcsHandler.initialize(context);
    assertEndpointForbidden(status -> gcsHandler.testGCSConnection(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> gcsHandler.exploreGCS(request, createMockResponder(status), TEST_NS, "gcs1", "/", 10));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "gcs1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> gcsHandler.loadObject(request, createMockResponder(status), TEST_NS, "gcs1", "b1", "p1", "default"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "gcs1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> gcsHandler.specification(request, createMockResponder(status), TEST_NS, "gcs1", "ws1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "gcs1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> gcsHandler.specification(request, createMockResponder(status), TEST_NS, "gcs1", null));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "gcs1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());

    // SpannerHandler
    SpannerHandler spannerHandler = new SpannerHandler();
    spannerHandler.initialize(context);
    assertEndpointForbidden(
      status -> spannerHandler.testSpannerConnection(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> spannerHandler.getSpannerInstances(request, createMockResponder(status), TEST_NS, "sp1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "sp1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> spannerHandler.getSpannerDatabases(request, createMockResponder(status), TEST_NS, "sp1", "i1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "sp1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> spannerHandler.getSpannerTables(request, createMockResponder(status), TEST_NS, "sp1", "i1", "db1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "sp1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> spannerHandler.readTable(
        request, createMockResponder(status), TEST_NS, "sp1", "i1", "db1", "t1", "default", "10"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "sp1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> spannerHandler.specification(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // DatabaseHandler
    DatabaseHandler dbHandler = new DatabaseHandler();
    dbHandler.initialize(context);
    assertEndpointForbidden(status -> dbHandler.listDrivers(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.LIST, EntityType.ARTIFACT),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dbHandler.listAvailableDrivers(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dbHandler.testConnection(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dbHandler.listDatabases(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dbHandler.listTables(request, createMockResponder(status), TEST_NS, "db1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "db1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> dbHandler.read(request, createMockResponder(status), TEST_NS, "db1", "t1", 10, "default"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "db1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> dbHandler.specification(request, createMockResponder(status), TEST_NS, "db1", "t1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "db1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());

    // KafkaHandler
    KafkaHandler kafkaHandler = new KafkaHandler();
    kafkaHandler.initialize(context);
    assertEndpointForbidden(status -> kafkaHandler.test(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> kafkaHandler.list(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> kafkaHandler.read(request, createMockResponder(status), TEST_NS, "k1", "topic1", 10, "default"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "k1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> kafkaHandler.specification(request, createMockResponder(status), TEST_NS, "k1", "topic1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "k1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());

    // ADLSHandler
    ADLSHandler adlsHandler = new ADLSHandler();
    adlsHandler.initialize(context);
    assertEndpointForbidden(status -> adlsHandler.testADLSConnection(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> adlsHandler.listADLSDirectory(request, createMockResponder(status), TEST_NS, "a1", "/"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "a1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> adlsHandler.loadADLSFile(
        request, createMockResponder(status), TEST_NS, "a1", "/file", 10, "FIRST", 1.0, "default"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "a1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> adlsHandler.specification(request, createMockResponder(status), TEST_NS, "a1", "/file", "ws1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "a1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> adlsHandler.specification(request, createMockResponder(status), TEST_NS, "a1", "/file", null));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "a1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());

    // S3Handler
    S3Handler s3Handler = new S3Handler();
    s3Handler.initialize(context);
    assertEndpointForbidden(status -> s3Handler.testS3Connection(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> s3Handler.getS3BucketInfo(request, createMockResponder(status), TEST_NS, "s31", "/", 10));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "s31"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> s3Handler.loadObject(
        request, createMockResponder(status), TEST_NS, "s31", "b1", "k1", 10, "FIRST", 1.0, "default"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "s31"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> s3Handler.loadObject(
        request, createMockResponder(status), TEST_NS, "s31", "b1", "", 10, "FIRST", 1.0, "default"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "s31"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> s3Handler.specification(request, createMockResponder(status), TEST_NS, "s31", "b1", "k1", "ws1"));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "s31"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> s3Handler.specification(request, createMockResponder(status), TEST_NS, "s31", "b1", "k1", null));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "s31"), StandardPermission.USE),
                        denyEnforcer.getLastCall());

    // Verify secondary WorkspaceEntityId checks on multi-check connector endpoints
    RecordingAccessEnforcer denyWorkspaceOnlyEnforcer = new RecordingAccessEnforcer(false) {
      @Override
      public void enforce(EntityId entity, Permission permission) throws AccessException {
        super.enforce(entity, permission);
        if (entity instanceof WorkspaceEntityId) {
          throw new UnauthorizedException("Denied on workspace");
        }
      }
    };
    SystemHttpServiceContext wsCheckContext = createMockContext(denyWorkspaceOnlyEnforcer, true);

    BigQueryHandler bqWsHandler = new BigQueryHandler();
    bqWsHandler.initialize(wsCheckContext);
    assertEndpointForbidden(
      status -> bqWsHandler.readTable(request, createMockResponder(status), TEST_NS, "bq1", "ds1", "t1", "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyWorkspaceOnlyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> bqWsHandler.specification(request, createMockResponder(status), TEST_NS, "bq1", "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyWorkspaceOnlyEnforcer.getLastCall());

    GCSHandler gcsWsHandler = new GCSHandler();
    gcsWsHandler.initialize(wsCheckContext);
    assertEndpointForbidden(
      status -> gcsWsHandler.loadObject(request, createMockResponder(status), TEST_NS, "gcs1", "b1", "p1", "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyWorkspaceOnlyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> gcsWsHandler.specification(request, createMockResponder(status), TEST_NS, "gcs1", "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyWorkspaceOnlyEnforcer.getLastCall());

    SpannerHandler spannerWsHandler = new SpannerHandler();
    spannerWsHandler.initialize(wsCheckContext);
    assertEndpointForbidden(
      status -> spannerWsHandler.readTable(
        request, createMockResponder(status), TEST_NS, "sp1", "i1", "db1", "t1", "default", "10"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyWorkspaceOnlyEnforcer.getLastCall());

    DatabaseHandler dbWsHandler = new DatabaseHandler();
    dbWsHandler.initialize(wsCheckContext);
    assertEndpointForbidden(
      status -> dbWsHandler.read(request, createMockResponder(status), TEST_NS, "db1", "t1", 10, "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyWorkspaceOnlyEnforcer.getLastCall());

    KafkaHandler kafkaWsHandler = new KafkaHandler();
    kafkaWsHandler.initialize(wsCheckContext);
    assertEndpointForbidden(
      status -> kafkaWsHandler.read(request, createMockResponder(status), TEST_NS, "k1", "topic1", 10, "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyWorkspaceOnlyEnforcer.getLastCall());

    ADLSHandler adlsWsHandler = new ADLSHandler();
    adlsWsHandler.initialize(wsCheckContext);
    assertEndpointForbidden(
      status -> adlsWsHandler.loadADLSFile(
        request, createMockResponder(status), TEST_NS, "a1", "/file", 10, "FIRST", 1.0, "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyWorkspaceOnlyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> adlsWsHandler.specification(request, createMockResponder(status), TEST_NS, "a1", "/file", "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyWorkspaceOnlyEnforcer.getLastCall());

    S3Handler s3WsHandler = new S3Handler();
    s3WsHandler.initialize(wsCheckContext);
    assertEndpointForbidden(
      status -> s3WsHandler.loadObject(
        request, createMockResponder(status), TEST_NS, "s31", "b1", "k1", 10, "FIRST", 1.0, "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyWorkspaceOnlyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> s3WsHandler.specification(request, createMockResponder(status), TEST_NS, "s31", "b1", "k1", "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyWorkspaceOnlyEnforcer.getLastCall());
  }

  @Test
  public void testConnectorSpecificationNullWorkspaceIdAllowPaths() throws Exception {
    RecordingAccessEnforcer allowEnforcer = new RecordingAccessEnforcer(false);
    SystemHttpServiceContext allowContext = createMockContext(allowEnforcer, true);
    ConnectionTypeConfig emptyConfig = new ConnectionTypeConfig(Collections.emptySet(), Collections.emptyList(), null);
    ConnectionHandler connHandler = createConnectionHandler(emptyConfig, allowEnforcer, true);
    HttpServiceRequest emptyReq = createMockRequest("{}");

    // GCS specification with null wid returns 400 after enforcing connection USE
    GCSHandler gcsHandler = new GCSHandler();
    gcsHandler.initialize(allowContext);
    assertEndpointStatus(HttpURLConnection.HTTP_BAD_REQUEST,
                         status -> gcsHandler.specification(emptyReq, createMockResponder(status),
                                                            TEST_NS, "gcs1", null));

    // S3 loadObject with empty key returns 400 after enforcing connection USE and workspace CREATE
    S3Handler s3Handler = new S3Handler();
    s3Handler.initialize(allowContext);
    assertEndpointStatus(HttpURLConnection.HTTP_BAD_REQUEST,
                         status -> s3Handler.loadObject(emptyReq, createMockResponder(status),
                                                        TEST_NS, "s31", "b1", "", 10, "FIRST", 1.0, "default"));

    // ADLS and S3 specification with null wid return 200 OK without WorkspaceEntityId NPE or shouldCopyHeader failure
    HttpServiceRequest createAdlsReq = createMockRequest(
      "{\"name\":\"adls_spec\",\"type\":\"ADLS\",\"properties\":{"
      + "\"clientID\":\"cid\",\"clientSecret\":\"sec\",\"refreshURL\":\"https://login/token\","
      + "\"accountFQDN\":\"acct.azuredatalakestore.net\"}}");
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> connHandler.create(createAdlsReq, createMockResponder(status), TEST_NS));
    ADLSHandler adlsHandler = new ADLSHandler();
    adlsHandler.initialize(allowContext);
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> adlsHandler.specification(emptyReq, createMockResponder(status),
                                                             TEST_NS, "adls_spec", "/dir/file.csv", null));

    HttpServiceRequest createS3Req = createMockRequest(
      "{\"name\":\"s3_spec\",\"type\":\"S3\",\"properties\":{"
      + "\"accessKeyId\":\"ak\",\"accessSecretKey\":\"sk\",\"region\":\"us-east-1\"}}");
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> connHandler.create(createS3Req, createMockResponder(status), TEST_NS));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> s3Handler.specification(emptyReq, createMockResponder(status),
                                                           TEST_NS, "s3_spec", "my-bucket", "dir/file.csv", null));
  }

  @Test
  public void testExplorerSchemaAndDirectiveHandlersRbacEnforcement() throws Exception {
    RecordingAccessEnforcer denyEnforcer = new RecordingAccessEnforcer(true);
    SystemHttpServiceContext context = createMockContext(denyEnforcer, true);
    HttpServiceRequest request = createMockRequest("{}");

    // FilesystemExplorer
    FilesystemExplorer fsExplorer = new FilesystemExplorer();
    fsExplorer.initialize(context);
    assertEndpointForbidden(status -> fsExplorer.list(request, createMockResponder(status), TEST_NS, "/", false));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> fsExplorer.read(request, createMockResponder(status), TEST_NS, "/file", 10, "FIRST", 1.0, "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> fsExplorer.specification(request, createMockResponder(status), TEST_NS, "/file", "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> fsExplorer.specification(request, createMockResponder(status), TEST_NS, "/file", null));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // DataModelHandler
    DataModelHandler dmHandler = new DataModelHandler();
    dmHandler.initialize(context);
    assertEndpointForbidden(status -> dmHandler.upload(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(InstanceId.SELF, StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dmHandler.listSchemas(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dmHandler.listSchema(request, createMockResponder(status), TEST_NS, "m1", 1L));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // SchemaRegistryHandler
    SchemaRegistryHandler srHandler = new SchemaRegistryHandler();
    srHandler.initialize(context);
    assertEndpointForbidden(
      status -> srHandler.create(request, createMockResponder(status), TEST_NS, "s1", "n1", "d1", "AVRO"));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.upload(request, createMockResponder(status), TEST_NS, "s1"));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.delete(request, createMockResponder(status), TEST_NS, "s1"));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.delete(request, createMockResponder(status), TEST_NS, "s1", 1L));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.get(request, createMockResponder(status), TEST_NS, "s1"));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.get(request, createMockResponder(status), TEST_NS, "s1", 1L));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.versions(request, createMockResponder(status), TEST_NS, "s1"));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // DirectivesHandler
    DirectivesHandler directivesHandler = new DirectivesHandler();
    directivesHandler.initialize(context);
    assertEndpointForbidden(
      status -> directivesHandler.create(request, createMockResponder(status), TEST_NS, "ws1", "default", "text"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.list(request, createMockResponder(status), TEST_NS, "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.LIST),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.delete(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.DELETE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.deleteGroup(request, createMockResponder(status), TEST_NS, "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.LIST),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.get(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.upload(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(StandardPermission.CREATE, denyEnforcer.getLastCall().permission);
    Assert.assertTrue(denyEnforcer.getLastCall().entityId instanceof WorkspaceEntityId);
    assertEndpointForbidden(
      status -> directivesHandler.uploadData(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.execute(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.summary(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.schema(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.addDataModel(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.removeDataModel(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.addModels(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.removeModels(request, createMockResponder(status), TEST_NS, "ws1", "m1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.capabilities(request, createMockResponder(status)));
    Assert.assertEquals(new EnforceCall(NamespaceId.SYSTEM, StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.usage(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.artifacts(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.LIST, EntityType.ARTIFACT),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.directives(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.LIST, EntityType.ARTIFACT),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.directivesReload(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.charsets(request, createMockResponder(status)));
    Assert.assertEquals(new EnforceCall(NamespaceId.SYSTEM, StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.uploadConfig(request, createMockResponder(status)));
    Assert.assertEquals(new EnforceCall(new DirectiveConfigEntityId(), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.getConfig(request, createMockResponder(status)));
    Assert.assertEquals(new EnforceCall(new DirectiveConfigEntityId(), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // WorkspaceHandler (v2)
    WorkspaceHandler wsHandler = new WorkspaceHandler();
    wsHandler.initialize(context);
    assertEndpointForbidden(status -> wsHandler.createWorkspace(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(StandardPermission.CREATE, denyEnforcer.getLastCall().permission);
    assertEndpointForbidden(status -> wsHandler.listWorkspaces(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "*"), StandardPermission.LIST),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> wsHandler.getWorkspace(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> wsHandler.updateWorkspace(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> wsHandler.resampleWorkspace(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> wsHandler.deleteWorkspace(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.DELETE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> wsHandler.upload(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(StandardPermission.CREATE, denyEnforcer.getLastCall().permission);
    assertEndpointForbidden(status -> wsHandler.getDirectives(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> wsHandler.execute(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> wsHandler.specification(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> wsHandler.applyRecipe(request, createMockResponder(status), TEST_NS, "ws1", "r1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());

    // Verify applyRecipe also enforces RecipeEntityId(ns, recipeId) GET when workspace USE is allowed
    RecordingAccessEnforcer denyRecipeOnlyEnforcer = new RecordingAccessEnforcer(false) {
      @Override
      public void enforce(EntityId entity, Permission permission) throws AccessException {
        super.enforce(entity, permission);
        if (entity instanceof RecipeEntityId) {
          throw new UnauthorizedException("Denied on recipe");
        }
      }
    };
    WorkspaceHandler wsHandlerRecipeCheck = new WorkspaceHandler();
    wsHandlerRecipeCheck.initialize(createMockContext(denyRecipeOnlyEnforcer, true));
    assertEndpointForbidden(
      status -> wsHandlerRecipeCheck.applyRecipe(request, createMockResponder(status), TEST_NS, "ws1", "r1"));
    Assert.assertEquals(new EnforceCall(new RecipeEntityId(TEST_NS, "r1"), StandardPermission.GET),
                        denyRecipeOnlyEnforcer.getLastCall());

    // RecipeHandler (v2)
    RecipeHandler recipeHandler = new RecipeHandler();
    recipeHandler.initialize(context);
    assertEndpointForbidden(status -> recipeHandler.createRecipe(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(StandardPermission.CREATE, denyEnforcer.getLastCall().permission);
    Assert.assertTrue(denyEnforcer.getLastCall().entityId instanceof RecipeEntityId);
    assertEndpointForbidden(
      status -> recipeHandler.getRecipeById(request, createMockResponder(status), TEST_NS, "r1"));
    Assert.assertEquals(new EnforceCall(new RecipeEntityId(TEST_NS, "r1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> recipeHandler.getRecipeByName(request, createMockResponder(status), TEST_NS, "rname"));
    Assert.assertEquals(new EnforceCall(new RecipeEntityId(TEST_NS, "*"), StandardPermission.LIST),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> recipeHandler.listRecipes(request, createMockResponder(status), TEST_NS, 10, null, null, null));
    Assert.assertEquals(new EnforceCall(new RecipeEntityId(TEST_NS, "*"), StandardPermission.LIST),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> recipeHandler.deleteRecipe(request, createMockResponder(status), TEST_NS, "r1"));
    Assert.assertEquals(new EnforceCall(new RecipeEntityId(TEST_NS, "r1"), StandardPermission.DELETE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> recipeHandler.updateRecipe(request, createMockResponder(status), TEST_NS, "r1"));
    Assert.assertEquals(new EnforceCall(new RecipeEntityId(TEST_NS, "r1"), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());

    // Verify WRANGLER_WORKSPACE_AUTH_CHECK=false disables the 6 legacy DirectivesHandler metadata checks
    RecordingAccessEnforcer wsDisabledEnforcer = new RecordingAccessEnforcer(true);
    DirectivesHandler wsDisabledHandler = new DirectivesHandler();
    wsDisabledHandler.initialize(createMockContext(wsDisabledEnforcer, false));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> wsDisabledHandler.capabilities(request, createMockResponder(status)));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> wsDisabledHandler.charsets(request, createMockResponder(status)));
    Assert.assertNull(wsDisabledEnforcer.getLastCall());
  }

  @Test
  public void testRecipeByNameWorkspaceOverwriteAndConnectionDiscoverer() throws Exception {
    // 1. RecipeHandler.getRecipeByName: single-recipe UUID GET without LIST succeeds; missing recipe requires LIST
    RecipeHandler allowRecipeHandler = createRecipeHandler(new RecordingAccessEnforcer(false), true);
    HttpServiceRequest createRecipeReq = createMockRequest(
      "{\"recipeName\":\"My Recipe\",\"description\":\"desc\",\"directives\":[\"parse-as-csv :body , false\"]}");
    AtomicInteger createStatus = new AtomicInteger();
    AtomicReference<Object> createdObj = new AtomicReference<>();
    allowRecipeHandler.createRecipe(createRecipeReq, createMockResponder(createStatus, createdObj), TEST_NS);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, createStatus.get());
    Recipe createdRecipe = (Recipe) createdObj.get();
    String recipeUuid = createdRecipe.getRecipeId().getRecipeId();
    RecipeEntityId targetRecipeEntity = new RecipeEntityId(TEST_NS, recipeUuid);

    RecordingAccessEnforcer singleRecipeGetOnly = new RecordingAccessEnforcer(false) {
      @Override
      public void enforce(EntityId entity, Permission permission) throws AccessException {
        super.enforce(entity, permission);
        if (!(targetRecipeEntity.equals(entity) && StandardPermission.GET.equals(permission))) {
          throw new UnauthorizedException("Only GET on target recipe UUID is granted");
        }
      }
    };
    RecipeHandler singleGetHandler = createRecipeHandler(singleRecipeGetOnly, true);
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> singleGetHandler.getRecipeByName(createRecipeReq, createMockResponder(status),
                                                                    TEST_NS, "My Recipe"));
    assertEndpointForbidden(
      status -> singleGetHandler.getRecipeByName(createRecipeReq, createMockResponder(status),
                                                 TEST_NS, "Missing Recipe"));
    Assert.assertEquals(new EnforceCall(new RecipeEntityId(TEST_NS, "*"), StandardPermission.LIST),
                        singleRecipeGetOnly.getLastCall());

    RecordingAccessEnforcer denyUuidGet = new RecordingAccessEnforcer(false) {
      @Override
      public void enforce(EntityId entity, Permission permission) throws AccessException {
        super.enforce(entity, permission);
        if (targetRecipeEntity.equals(entity) && StandardPermission.GET.equals(permission)) {
          throw new UnauthorizedException("Denied GET on resolved recipe UUID");
        }
      }
    };
    RecipeHandler denyUuidHandler = createRecipeHandler(denyUuidGet, true);
    assertEndpointForbidden(
      status -> denyUuidHandler.getRecipeByName(createRecipeReq, createMockResponder(status), TEST_NS, "My Recipe"));
    Assert.assertEquals(new EnforceCall(targetRecipeEntity, StandardPermission.GET), denyUuidGet.getLastCall());

    assertEndpointStatus(HttpURLConnection.HTTP_NOT_FOUND,
                         status -> allowRecipeHandler.getRecipeByName(createRecipeReq, createMockResponder(status),
                                                                      TEST_NS, "Missing Recipe"));

    // 2. DirectivesHandler workspace overwrite UPDATE and deleteGroup atomicity
    RecordingAccessEnforcer selectiveWsEnforcer = new RecordingAccessEnforcer(false) {
      @Override
      public void enforce(EntityId entity, Permission permission) throws AccessException {
        super.enforce(entity, permission);
        if (new WorkspaceEntityId(TEST_NS, "ws_overwrite").equals(entity)
            && StandardPermission.UPDATE.equals(permission)) {
          throw new UnauthorizedException("Denied UPDATE on existing workspace");
        }
        if (new WorkspaceEntityId(TEST_NS, "grp_ws2").equals(entity)
            && StandardPermission.DELETE.equals(permission)) {
          throw new UnauthorizedException("Denied DELETE on grp_ws2");
        }
      }
    };
    DirectivesHandler selectiveHandler = new DirectivesHandler();
    selectiveHandler.initialize(createMockContext(selectiveWsEnforcer, true));
    HttpServiceRequest emptyReq = createMockRequest("{}");

    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> selectiveHandler.create(emptyReq, createMockResponder(status),
                                                           TEST_NS, "ws_overwrite", "ws_overwrite", "default"));
    assertEndpointForbidden(
      status -> selectiveHandler.create(emptyReq, createMockResponder(status),
                                        TEST_NS, "ws_overwrite", "ws_overwrite", "default"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws_overwrite"), StandardPermission.UPDATE),
                        selectiveWsEnforcer.getLastCall());

    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> selectiveHandler.create(emptyReq, createMockResponder(status),
                                                           TEST_NS, "grp_ws1", "grp_ws1", "grp1"));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> selectiveHandler.create(emptyReq, createMockResponder(status),
                                                           TEST_NS, "grp_ws2", "grp_ws2", "grp1"));
    assertEndpointForbidden(
      status -> selectiveHandler.deleteGroup(emptyReq, createMockResponder(status), TEST_NS, "grp1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "grp_ws2"), StandardPermission.DELETE),
                        selectiveWsEnforcer.getLastCall());
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> selectiveHandler.get(emptyReq, createMockResponder(status), TEST_NS, "grp_ws1"));

    DirectivesHandler allowDirectivesHandler = new DirectivesHandler();
    allowDirectivesHandler.initialize(createMockContext(new RecordingAccessEnforcer(false), true));
    assertEndpointStatus(HttpURLConnection.HTTP_OK,
                         status -> allowDirectivesHandler.deleteGroup(emptyReq, createMockResponder(status),
                                                                      TEST_NS, "grp1"));
    assertEndpointStatus(HttpURLConnection.HTTP_NOT_FOUND,
                         status -> allowDirectivesHandler.get(emptyReq, createMockResponder(status),
                                                              TEST_NS, "grp_ws1"));

    // 3. ConnectionDiscoverer propagates HTTP 403 immediately without retrying
    AtomicInteger attempts = new AtomicInteger();
    ServiceDiscoverer forbiddenDiscoverer = (ServiceDiscoverer) Proxy.newProxyInstance(
      ServiceDiscoverer.class.getClassLoader(),
      new Class<?>[]{ServiceDiscoverer.class},
      (proxy, method, args) -> {
        if ("openConnection".equals(method.getName())) {
          attempts.incrementAndGet();
          return new StubHttpURLConnection(HttpURLConnection.HTTP_FORBIDDEN, "Forbidden by RBAC");
        }
        return null;
      });
    ConnectionDiscoverer discoverer = new ConnectionDiscoverer(forbiddenDiscoverer);
    try {
      discoverer.retrieveSample(TEST_NS, "conn1", null);
      Assert.fail("Expected StatusCodeException on HTTP 403");
    } catch (StatusCodeException e) {
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, e.getCode());
      Assert.assertEquals(1, attempts.get());
    }
  }

  private interface EndpointInvocation {
    void invoke(AtomicInteger statusSink) throws Exception;
  }

  private static void assertEndpointForbidden(EndpointInvocation invocation) throws Exception {
    assertEndpointStatus(HttpURLConnection.HTTP_FORBIDDEN, invocation);
  }

  private static void assertEndpointStatus(int expectedStatus, EndpointInvocation invocation) throws Exception {
    AtomicInteger status = new AtomicInteger();
    invocation.invoke(status);
    Assert.assertEquals(expectedStatus, status.get());
  }

  private static RecipeHandler createRecipeHandler(ContextAccessEnforcer enforcer,
                                                   boolean featureFlagsEnabled) throws Exception {
    RecipeHandler handler = new RecipeHandler();
    handler.initialize(createMockContext(enforcer, featureFlagsEnabled));
    java.lang.reflect.Field metricsField = RecipeHandler.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    Metrics noopMetrics = (Metrics) Proxy.newProxyInstance(
      Metrics.class.getClassLoader(), new Class<?>[]{Metrics.class}, (proxy, method, args) -> null);
    metricsField.set(handler, noopMetrics);
    return handler;
  }

  @SuppressWarnings("unchecked")
  private static StructuredTable createInMemoryTable(StructuredTableSpecification spec) {
    StructuredTableSchema schema = new StructuredTableSchema(spec);
    FieldValidator validator = new FieldValidator(schema);
    Map<List<Object>, Map<String, Object>> rows = new LinkedHashMap<>();

    return (StructuredTable) Proxy.newProxyInstance(
      StructuredTable.class.getClassLoader(),
      new Class<?>[]{StructuredTable.class},
      (proxy, method, args) -> {
        switch (method.getName()) {
          case "upsert": {
            Collection<Field<?>> fields = (Collection<Field<?>>) args[0];
            fields.forEach(validator::validateField);
            Map<String, Object> row = new HashMap<>();
            for (Field<?> field : fields) {
              row.put(field.getName(), field.getValue());
            }
            List<Object> pk = new ArrayList<>();
            for (String pkCol : schema.getPrimaryKeys()) {
              pk.add(row.get(pkCol));
            }
            rows.put(pk, row);
            return null;
          }
          case "read": {
            Collection<Field<?>> keys = (Collection<Field<?>>) args[0];
            validator.validatePrimaryKeys(keys, false);
            List<Object> pk = new ArrayList<>();
            for (Field<?> field : keys) {
              pk.add(field.getValue());
            }
            Map<String, Object> found = rows.get(pk);
            return found == null ? Optional.empty() : Optional.of(createStructuredRow(found));
          }
          case "delete": {
            Collection<Field<?>> keys = (Collection<Field<?>>) args[0];
            validator.validatePrimaryKeys(keys, false);
            List<Object> pk = new ArrayList<>();
            for (Field<?> field : keys) {
              pk.add(field.getValue());
            }
            rows.remove(pk);
            return null;
          }
          case "scan": {
            Range range = (Range) args[0];
            int limit = args.length > 1 && args[1] instanceof Integer ? (Integer) args[1] : Integer.MAX_VALUE;
            List<StructuredRow> matched = new ArrayList<>();
            for (Map<String, Object> row : rows.values()) {
              boolean matches = true;
              for (Field<?> filter : range.getBegin()) {
                if (!Objects.equals(row.get(filter.getName()), filter.getValue())) {
                  matches = false;
                  break;
                }
              }
              if (matches) {
                matched.add(createStructuredRow(row));
                if (matched.size() >= limit) {
                  break;
                }
              }
            }
            Iterator<StructuredRow> iterator = matched.iterator();
            return new CloseableIterator<StructuredRow>() {
              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public StructuredRow next() {
                return iterator.next();
              }

              @Override
              public void close() {
              }
            };
          }
          default:
            return null;
        }
      });
  }

  private static StructuredRow createStructuredRow(Map<String, Object> row) {
    return (StructuredRow) Proxy.newProxyInstance(
      StructuredRow.class.getClassLoader(),
      new Class<?>[]{StructuredRow.class},
      (proxy, method, args) -> args != null && args.length == 1 && args[0] instanceof String
        ? row.get((String) args[0]) : null);
  }

  private static ConnectionHandler createConnectionHandler(ConnectionTypeConfig config,
                                                           ContextAccessEnforcer enforcer,
                                                           boolean featureFlagsEnabled) throws Exception {
    ConnectionHandler handler = new ConnectionHandler(config);
    Map<String, String> specProps = Collections.singletonMap("connectionTypeConfig", GSON.toJson(config));
    handler.initialize(createMockContext(enforcer, featureFlagsEnabled, specProps));
    return handler;
  }

  private static SystemHttpServiceContext createMockContext(ContextAccessEnforcer enforcer,
                                                            boolean featureFlagsEnabled) {
    return createMockContext(enforcer, featureFlagsEnabled, Collections.emptyMap());
  }

  private static SystemHttpServiceContext createMockContext(ContextAccessEnforcer enforcer,
                                                            boolean featureFlagsEnabled,
                                                            Map<String, String> specProperties) {
    Set<Feature> enabled = featureFlagsEnabled ? EnumSet.allOf(Feature.class) : EnumSet.noneOf(Feature.class);
    return createMockContext(enforcer, enabled, specProperties);
  }

  private static SystemHttpServiceContext createMockContext(ContextAccessEnforcer enforcer,
                                                            Set<Feature> enabledFeatures,
                                                            Map<String, String> specProperties) {
    Admin admin = (Admin) Proxy.newProxyInstance(
      Admin.class.getClassLoader(),
      new Class<?>[]{Admin.class},
      (proxy, method, args) -> {
        if ("getNamespaceSummary".equals(method.getName())) {
          String ns = (String) args[0];
          if (MISSING_NS.equals(ns)) {
            return null;
          }
          return new NamespaceSummary(ns, "", 1L);
        }
        return null;
      });

    HttpServiceHandlerSpecification spec = new HttpServiceHandlerSpecification(
      "", "", "", specProperties, Collections.emptySet(), Collections.emptyList());

    return (SystemHttpServiceContext) Proxy.newProxyInstance(
      SystemHttpServiceContext.class.getClassLoader(),
      new Class<?>[]{SystemHttpServiceContext.class},
      (proxy, method, args) -> {
        switch (method.getName()) {
          case "getContextAccessEnforcer":
            return enforcer;
          case "isFeatureEnabled":
            String flagName = (String) args[0];
            return enabledFeatures.stream().anyMatch(f -> f.getFeatureFlagString().equals(flagName));
          case "getAdmin":
            return admin;
          case "getSpecification":
            return spec;
          case "getRuntimeArguments":
            return new HashMap<String, String>();
          case "listArtifacts":
            return Collections.emptyList();
          case "run":
            transactionRunner.run((TxRunnable) args[0]);
            return null;
          default:
            return null;
        }
      });
  }

  private static HttpServiceRequest createMockRequest(String body) {
    return (HttpServiceRequest) Proxy.newProxyInstance(
      HttpServiceRequest.class.getClassLoader(),
      new Class<?>[]{HttpServiceRequest.class},
      (proxy, method, args) -> {
        if ("getContent".equals(method.getName())) {
          return ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8));
        }
        if ("getHeader".equals(method.getName()) && args != null && args.length > 0
            && PropertyIds.FILE_NAME.equals(args[0])) {
          return "sample.csv";
        }
        return null;
      });
  }

  private static HttpServiceResponder createMockResponder(AtomicInteger statusSink) {
    return createMockResponder(statusSink, null);
  }

  private static HttpServiceResponder createMockResponder(AtomicInteger statusSink,
                                                          AtomicReference<Object> bodySink) {
    return (HttpServiceResponder) Proxy.newProxyInstance(
      HttpServiceResponder.class.getClassLoader(),
      new Class<?>[]{HttpServiceResponder.class},
      (proxy, method, args) -> {
        if (method.getName().startsWith("send") && args != null && args.length > 0) {
          if (args[0] instanceof Integer) {
            statusSink.set((Integer) args[0]);
            if (bodySink != null && args.length > 1) {
              bodySink.set(args[1]);
            }
          } else if ("sendJson".equals(method.getName()) || "sendString".equals(method.getName())) {
            statusSink.set(HttpURLConnection.HTTP_OK);
            if (bodySink != null) {
              bodySink.set(args[0]);
            }
          }
        }
        return null;
      });
  }

  private static final class StubHttpURLConnection extends HttpURLConnection {
    private final int status;
    private final byte[] errorBytes;

    private StubHttpURLConnection(int status, String errorBody) throws IOException {
      super(new URL("http://localhost/stub"));
      this.status = status;
      this.errorBytes = errorBody.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int getResponseCode() {
      return status;
    }

    @Override
    public OutputStream getOutputStream() {
      return new ByteArrayOutputStream();
    }

    @Override
    public InputStream getErrorStream() {
      return new ByteArrayInputStream(errorBytes);
    }

    @Override
    public void disconnect() {
    }

    @Override
    public boolean usingProxy() {
      return false;
    }

    @Override
    public void connect() {
    }
  }

  private static final class EnforceCall {
    private final EntityId entityId;
    private final Permission permission;
    // Set only for enforceOnParent calls, where entityId is the parent.
    private final EntityType childType;

    private EnforceCall(EntityId entityId, Permission permission) {
      this(entityId, permission, null);
    }

    private EnforceCall(EntityId entityId, Permission permission, EntityType childType) {
      this.entityId = entityId;
      this.permission = permission;
      this.childType = childType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EnforceCall)) {
        return false;
      }
      EnforceCall that = (EnforceCall) o;
      return entityId.equals(that.entityId) && permission.equals(that.permission)
        && Objects.equals(childType, that.childType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entityId, permission, childType);
    }

    @Override
    public String toString() {
      return entityId + ":" + permission + (childType == null ? "" : " on children of type " + childType);
    }
  }

  private static class RecordingAccessEnforcer implements ContextAccessEnforcer {
    private final boolean denyAll;
    private final List<EnforceCall> calls = new ArrayList<>();

    private RecordingAccessEnforcer(boolean denyAll) {
      this.denyAll = denyAll;
    }

    @Override
    public void enforce(EntityId entity, Permission permission) throws AccessException {
      calls.add(new EnforceCall(entity, permission));
      if (denyAll) {
        throw new UnauthorizedException("Access denied for " + entity + " with " + permission);
      }
    }

    @Override
    public void enforce(EntityId entity, Set<? extends Permission> permissions) throws AccessException {
      for (Permission permission : permissions) {
        enforce(entity, permission);
      }
    }

    @Override
    public void enforceOnParent(EntityType entityType, EntityId parentId, Permission permission)
      throws AccessException {
      calls.add(new EnforceCall(parentId, permission, entityType));
      if (denyAll) {
        throw new UnauthorizedException("Access denied on parent " + parentId + " with " + permission);
      }
    }

    @Override
    public Set<? extends EntityId> isVisible(Set<? extends EntityId> entities) throws AccessException {
      return entities;
    }

    EnforceCall getLastCall() {
      return calls.isEmpty() ? null : calls.get(calls.size() - 1);
    }

    List<EnforceCall> getCalls() {
      return new ArrayList<>(calls);
    }

    void clear() {
      calls.clear();
    }
  }

  private static final class TestWranglerHandler extends AbstractWranglerHandler {
    void callRespondNoNamespace(HttpServiceRequest request, HttpServiceResponder responder,
                                Callable<Object> callable) {
      respond(request, responder, callable::call);
    }

    void callRespondConsumer(HttpServiceResponder responder, String namespace,
                             Callable<Void> callable) {
      respond(responder, namespace, ns -> callable.call());
    }

    void callRespondFunction(HttpServiceRequest request, HttpServiceResponder responder, String namespace,
                             Callable<Object> callable) {
      respond(request, responder, namespace, ns -> callable.call());
    }

    boolean checkShouldCopyHeader(NamespacedId workspaceId) throws IOException {
      return shouldCopyHeader(null, workspaceId);
    }
  }
}
