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
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ConnectionEntityId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.id.DirectiveConfigEntityId;
import io.cdap.wrangler.proto.id.RecipeEntityId;
import io.cdap.wrangler.proto.id.WorkspaceEntityId;
import io.cdap.wrangler.service.adls.ADLSHandler;
import io.cdap.wrangler.service.bigquery.BigQueryHandler;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.connections.ConnectionHandler;
import io.cdap.wrangler.service.connections.ConnectionTypeConfig;
import io.cdap.wrangler.service.database.DatabaseHandler;
import io.cdap.wrangler.service.directive.DirectivesHandler;
import io.cdap.wrangler.service.directive.WorkspaceHandler;
import io.cdap.wrangler.service.explorer.FilesystemExplorer;
import io.cdap.wrangler.service.gcs.GCSHandler;
import io.cdap.wrangler.service.kafka.KafkaHandler;
import io.cdap.wrangler.service.s3.S3Handler;
import io.cdap.wrangler.service.schema.DataModelHandler;
import io.cdap.wrangler.service.schema.SchemaRegistryHandler;
import io.cdap.wrangler.service.spanner.SpannerHandler;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for RBAC authorization enforcement and system namespace validation across Wrangler HTTP handlers.
 */
public class WranglerAuthEnforcementTest {
  private static final Gson GSON = new Gson();
  private static final String TEST_NS = "default";

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

    // 5. Overload 3: respond(request, responder, "system", function) returns 400
    AtomicInteger status3Sys = new AtomicInteger();
    handler.callRespondFunction(request, createMockResponder(status3Sys), "system", () -> {
      Assert.fail("System namespace should be rejected before function executes");
      return null;
    });
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, status3Sys.get());
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

    // 2. Verify preconfigured connection cannot be updated or deleted even when RBAC allows
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
    assertEndpointForbidden(
      status -> bqHandler.readTable(request, createMockResponder(status), TEST_NS, "bq1", "ds1", "t1", "default"));
    assertEndpointForbidden(
      status -> bqHandler.specification(request, createMockResponder(status), TEST_NS, "bq1", "ws1"));

    // GCSHandler
    GCSHandler gcsHandler = new GCSHandler();
    gcsHandler.initialize(context);
    assertEndpointForbidden(status -> gcsHandler.testGCSConnection(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new ConnectionEntityId(TEST_NS, "*"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> gcsHandler.exploreGCS(request, createMockResponder(status), TEST_NS, "gcs1", "/", 10));
    assertEndpointForbidden(
      status -> gcsHandler.loadObject(request, createMockResponder(status), TEST_NS, "gcs1", "b1", "p1", "default"));
    assertEndpointForbidden(
      status -> gcsHandler.specification(request, createMockResponder(status), TEST_NS, "gcs1", "ws1"));

    // SpannerHandler
    SpannerHandler spannerHandler = new SpannerHandler();
    spannerHandler.initialize(context);
    assertEndpointForbidden(
      status -> spannerHandler.testSpannerConnection(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(
      status -> spannerHandler.getSpannerInstances(request, createMockResponder(status), TEST_NS, "sp1"));
    assertEndpointForbidden(
      status -> spannerHandler.getSpannerDatabases(request, createMockResponder(status), TEST_NS, "sp1", "i1"));
    assertEndpointForbidden(
      status -> spannerHandler.getSpannerTables(request, createMockResponder(status), TEST_NS, "sp1", "i1", "db1"));
    assertEndpointForbidden(
      status -> spannerHandler.readTable(
        request, createMockResponder(status), TEST_NS, "sp1", "i1", "db1", "t1", "default", "10"));
    assertEndpointForbidden(
      status -> spannerHandler.specification(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // DatabaseHandler
    DatabaseHandler dbHandler = new DatabaseHandler();
    dbHandler.initialize(context);
    assertEndpointForbidden(status -> dbHandler.listDrivers(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dbHandler.listAvailableDrivers(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(status -> dbHandler.testConnection(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(status -> dbHandler.listDatabases(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(status -> dbHandler.listTables(request, createMockResponder(status), TEST_NS, "db1"));
    assertEndpointForbidden(
      status -> dbHandler.read(request, createMockResponder(status), TEST_NS, "db1", "t1", 10, "default"));
    assertEndpointForbidden(
      status -> dbHandler.specification(request, createMockResponder(status), TEST_NS, "db1", "t1"));

    // KafkaHandler
    KafkaHandler kafkaHandler = new KafkaHandler();
    kafkaHandler.initialize(context);
    assertEndpointForbidden(status -> kafkaHandler.test(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(status -> kafkaHandler.list(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(
      status -> kafkaHandler.read(request, createMockResponder(status), TEST_NS, "k1", "topic1", 10, "default"));
    assertEndpointForbidden(
      status -> kafkaHandler.specification(request, createMockResponder(status), TEST_NS, "k1", "topic1"));

    // ADLSHandler
    ADLSHandler adlsHandler = new ADLSHandler();
    adlsHandler.initialize(context);
    assertEndpointForbidden(status -> adlsHandler.testADLSConnection(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(
      status -> adlsHandler.listADLSDirectory(request, createMockResponder(status), TEST_NS, "a1", "/"));
    assertEndpointForbidden(
      status -> adlsHandler.loadADLSFile(
        request, createMockResponder(status), TEST_NS, "a1", "/file", 10, "FIRST", 1.0, "default"));
    assertEndpointForbidden(
      status -> adlsHandler.specification(request, createMockResponder(status), TEST_NS, "a1", "/file", "ws1"));

    // S3Handler
    S3Handler s3Handler = new S3Handler();
    s3Handler.initialize(context);
    assertEndpointForbidden(status -> s3Handler.testS3Connection(request, createMockResponder(status), TEST_NS));
    assertEndpointForbidden(
      status -> s3Handler.getS3BucketInfo(request, createMockResponder(status), TEST_NS, "s31", "/", 10));
    assertEndpointForbidden(
      status -> s3Handler.loadObject(
        request, createMockResponder(status), TEST_NS, "s31", "b1", "k1", 10, "FIRST", 1.0, "default"));
    assertEndpointForbidden(
      status -> s3Handler.specification(request, createMockResponder(status), TEST_NS, "s31", "b1", "k1", "ws1"));
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
    assertEndpointForbidden(
      status -> fsExplorer.specification(request, createMockResponder(status), TEST_NS, "/file", "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // DataModelHandler
    DataModelHandler dmHandler = new DataModelHandler();
    dmHandler.initialize(context);
    assertEndpointForbidden(status -> dmHandler.upload(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dmHandler.listSchemas(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> dmHandler.listSchema(request, createMockResponder(status), TEST_NS, "m1", 1L));

    // SchemaRegistryHandler
    SchemaRegistryHandler srHandler = new SchemaRegistryHandler();
    srHandler.initialize(context);
    assertEndpointForbidden(
      status -> srHandler.create(request, createMockResponder(status), TEST_NS, "s1", "n1", "d1", "AVRO"));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.upload(request, createMockResponder(status), TEST_NS, "s1"));
    assertEndpointForbidden(status -> srHandler.delete(request, createMockResponder(status), TEST_NS, "s1"));
    assertEndpointForbidden(status -> srHandler.delete(request, createMockResponder(status), TEST_NS, "s1", 1L));
    assertEndpointForbidden(status -> srHandler.get(request, createMockResponder(status), TEST_NS, "s1"));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(status -> srHandler.get(request, createMockResponder(status), TEST_NS, "s1", 1L));
    assertEndpointForbidden(status -> srHandler.versions(request, createMockResponder(status), TEST_NS, "s1"));

    // DirectivesHandler
    DirectivesHandler directivesHandler = new DirectivesHandler();
    directivesHandler.initialize(context);
    assertEndpointForbidden(
      status -> directivesHandler.create(request, createMockResponder(status), TEST_NS, "ws1", "default", "text"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.CREATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.execute(request, createMockResponder(status), TEST_NS, "ws1"));
    Assert.assertEquals(new EnforceCall(new WorkspaceEntityId(TEST_NS, "ws1"), StandardPermission.USE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.uploadConfig(request, createMockResponder(status)));
    Assert.assertEquals(new EnforceCall(new DirectiveConfigEntityId(), StandardPermission.UPDATE),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> directivesHandler.usage(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());

    // WorkspaceHandler (v2)
    WorkspaceHandler wsHandler = new WorkspaceHandler();
    wsHandler.initialize(context);
    assertEndpointForbidden(status -> wsHandler.getDirectives(request, createMockResponder(status), TEST_NS));
    Assert.assertEquals(new EnforceCall(new NamespaceId(TEST_NS), StandardPermission.GET),
                        denyEnforcer.getLastCall());
    assertEndpointForbidden(
      status -> wsHandler.execute(request, createMockResponder(status), TEST_NS, "ws1"));
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
  }

  private interface EndpointInvocation {
    void invoke(AtomicInteger statusSink) throws Exception;
  }

  private static void assertEndpointForbidden(EndpointInvocation invocation) throws Exception {
    AtomicInteger status = new AtomicInteger();
    invocation.invoke(status);
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, status.get());
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
    Admin admin = (Admin) Proxy.newProxyInstance(
      Admin.class.getClassLoader(),
      new Class<?>[]{Admin.class},
      (proxy, method, args) -> {
        if ("getNamespaceSummary".equals(method.getName())) {
          String ns = (String) args[0];
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
            return featureFlagsEnabled;
          case "getAdmin":
            return admin;
          case "getSpecification":
            return spec;
          case "getRuntimeArguments":
            return new HashMap<String, String>();
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
        return null;
      });
  }

  private static HttpServiceResponder createMockResponder(AtomicInteger statusSink) {
    return (HttpServiceResponder) Proxy.newProxyInstance(
      HttpServiceResponder.class.getClassLoader(),
      new Class<?>[]{HttpServiceResponder.class},
      (proxy, method, args) -> {
        if (method.getName().startsWith("send") && args != null && args.length > 0 && args[0] instanceof Integer) {
          statusSink.set((Integer) args[0]);
        }
        return null;
      });
  }

  private static final class EnforceCall {
    private final EntityId entityId;
    private final Permission permission;

    private EnforceCall(EntityId entityId, Permission permission) {
      this.entityId = entityId;
      this.permission = permission;
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
      return entityId.equals(that.entityId) && permission.equals(that.permission);
    }

    @Override
    public int hashCode() {
      return 31 * entityId.hashCode() + permission.hashCode();
    }

    @Override
    public String toString() {
      return entityId + ":" + permission;
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
      calls.add(new EnforceCall(parentId, permission));
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
  }
}
