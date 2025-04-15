package io.cdap.directives.validation;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.utils.Manifest;
import io.cdap.wrangler.utils.Manifest.Standard;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ValidateStandardTest {

  private static Map<String, Standard> getSpecsInArchive()
          throws IOException, NoSuchAlgorithmException, URISyntaxException {
    Map<String, Standard> schemas = new HashMap<>();
    CodeSource src = ValidateStandard.class.getProtectionDomain().getCodeSource();
    if (src != null) {
      File schemasRoot =
              Paths.get(src.getLocation().toURI()).resolve(ValidateStandard.SCHEMAS_RESOURCE_PATH).toFile();

      if (!schemasRoot.isDirectory()) {
        throw new IOException(
                String.format("Schemas root %s was not a directory", schemasRoot.getPath()));
      }

      for (File f : schemasRoot.listFiles()) {
        if (f.toPath().endsWith(ValidateStandard.MANIFEST_PATH)) {
          continue;
        }

        String hash = calcHash(new FileInputStream(f));
        schemas.put(
                FilenameUtils.getBaseName(f.getName()),
                new Standard(hash, FilenameUtils.getExtension(f.getName())));
      }
    }

    return schemas;
  }

  private static String calcHash(InputStream is) throws IOException, NoSuchAlgorithmException {
    byte[] bytes = IOUtils.toByteArray(is);
    MessageDigest d = MessageDigest.getInstance("SHA-256");
    byte[] hash = d.digest(bytes);

    Formatter f = new Formatter();
    for (byte b : hash) {
      f.format("%02x", b);
    }
    return f.toString();
  }

}
