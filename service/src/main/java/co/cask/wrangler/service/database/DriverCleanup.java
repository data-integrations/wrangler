package co.cask.wrangler.service.database;

import co.cask.cdap.etl.api.Destroyable;
import com.google.common.base.Throwables;

import java.sql.DriverManager;
import java.sql.SQLException;
import javax.annotation.Nullable;

public class DriverCleanup implements Destroyable {
  private final JDBCDriverShim driverShim;

  DriverCleanup(@Nullable JDBCDriverShim driverShim) {
    this.driverShim = driverShim;
  }

  public void destroy() {
    if (driverShim != null) {
      try {
        DriverManager.deregisterDriver(driverShim);
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}