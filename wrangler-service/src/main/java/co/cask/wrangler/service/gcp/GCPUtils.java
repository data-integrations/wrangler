package co.cask.wrangler.service.gcp;

import co.cask.wrangler.api.Pair;
import co.cask.wrangler.dataset.connections.Connection;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * Class description here.
 */
public final class GCPUtils {

  public static final String PROJECT_ID = "projectId";
  public static final String SERVICE_ACCOUNT_KEYFILE = "service-account-keyfile";

  public static ServiceAccountCredentials loadLocalFile(String path) throws Exception {
    File credentialsPath = new File(path);
    if (!credentialsPath.exists()) {
      throw new FileNotFoundException("Service account file " + credentialsPath.getName() + " does not exist.");
    }
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (FileNotFoundException e) {
      throw new Exception(
        String.format("Unable to find service account file '%s'.", path)
      );
    } catch (IOException e) {
      throw new Exception(
        String.format(
          "Issue reading service account file '%s', please check permission of the file", path
        )
      );
    }
  }

  public static Pair<String, ServiceAccountCredentials> getProjectIdAndCredentials(Connection connection)
    throws Exception {
    Map<String, Object> properties = connection.getAllProps();
    if (properties.get(GCPUtils.PROJECT_ID) == null) {
      throw new Exception("Configuration does not include project id.");
    }

    if (properties.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE) == null) {
      throw new Exception("Configuration does not include path to service account file.");
    }

    String path = (String) properties.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE);
    String projectId = (String) properties.get(GCPUtils.PROJECT_ID);
    ServiceAccountCredentials credentials = GCPUtils.loadLocalFile(path);

    return new Pair<>(projectId, credentials);
  }

  private GCPUtils() {
  }
}
