package co.cask.wrangler.service.database;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class JDBCDriverShim implements Driver {

  private final Driver delegate;

  public JDBCDriverShim(Driver delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return delegate.acceptsURL(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return delegate.connect(url, info);
  }

  @Override
  public int getMajorVersion() {
    return delegate.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return delegate.getMinorVersion();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return delegate.getPropertyInfo(url, info);
  }

  @Override
  public boolean jdbcCompliant() {
    return delegate.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }
}

