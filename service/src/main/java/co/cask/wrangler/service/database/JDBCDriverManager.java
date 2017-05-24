/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.service.database;

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.service.http.HttpServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * Class description here.
 */
public final class JDBCDriverManager {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCDriverManager.class);
  private static final String JDBC = "jdbc";
  private final String classz;
  private DriverCleanup cleanup;
  private final String url;
  private final HttpServiceContext context;
  private Connection connection;

  public JDBCDriverManager(String classz, HttpServiceContext context, String url) {
    this.classz = classz;
    this.cleanup = null;
    this.context = context;
    this.url = url;
  }

  public ArtifactInfo getArtifactInfo(String name) throws IOException {
    List<ArtifactInfo> artifactInfos = context.listArtifacts();
    ArtifactInfo targetArtifactInfo = null;
    for (ArtifactInfo artifactInfo : artifactInfos) {
      Set<PluginClass> pluginClassSet = artifactInfo.getClasses().getPlugins();
      for (PluginClass plugin : pluginClassSet) {
        if (JDBC.equalsIgnoreCase(plugin.getType()) && plugin.getName().equals(name)) {
          targetArtifactInfo = artifactInfo;
          break;
        }
      }
      if (targetArtifactInfo != null) {
        break;
      }
    }
    return targetArtifactInfo;
  }

  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> classz, String url)
    throws IllegalAccessException, InstantiationException, SQLException {
    try {
      DriverManager.getDriver(url);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      Driver driver = classz.newInstance();
      final JDBCDriverShim shim = new JDBCDriverShim(driver);
      try {
        deregisterAllDrivers(classz);
        DriverManager.registerDriver(shim);
        return new DriverCleanup(shim);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.warn("Unable to deregister JDBC Driver class {}", classz);
      }
      return null;
    }
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> classz)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = DBService.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.debug("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(classz.getClassLoader())) {
        list.remove(driverInfo);
      }
    }
  }

  public void release() {
    if (cleanup != null) {
      cleanup.destroy();
      cleanup = null;
    }
  }
}
