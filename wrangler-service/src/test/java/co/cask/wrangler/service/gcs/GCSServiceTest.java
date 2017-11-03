package co.cask.wrangler.service.gcs;

import co.cask.wrangler.BytesDecoder;
import co.cask.wrangler.service.FileTypeDetector;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

/**
 * Tests parts of {@link GCSService}
 */
public class GCSServiceTest {

  @Test
  @Ignore
  public void testReadingDataFromGCS() throws Exception {
    FileTypeDetector detector = new FileTypeDetector();
    Storage storage = getStorage("cask-dev-clusters", "/tmp/cask-dev-clusters-729251f6adf7.json");
    String bucket = "cdap";
    String path = "demo/csv/titanic.csv";

    Blob blob = storage.get(BlobId.of(bucket, path));
    if (!blob.isDirectory()) {
      String blobName = blob.getName();
      File file = new File(blobName);
      String fileType = detector.detectFileType(blobName);

      try (ReadChannel reader = blob.reader()) {
        int min = Math.min(blob.getSize().intValue(), GCSService.FILE_SIZE);
        reader.setChunkSize(min);
        byte[] bytes = new byte[min];
        WritableByteChannel writable = Channels.newChannel(new ByteArrayOutputStream(min));
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        long total = min;
        while (reader.read(buf) != -1 && total > 0) {
          buf.flip();
          while (buf.hasRemaining()) {
            total -= writable.write(buf);
          }
          buf.clear();
        }
        String encoding = BytesDecoder.guessEncoding(bytes);
        if (fileType.equalsIgnoreCase("text/plain")
          && (encoding.equalsIgnoreCase("utf-8") || encoding.equalsIgnoreCase("ascii"))) {
          String data = new String(bytes, encoding);
          String[] lines = data.split("\r\n|\r|\n");
          if (blob.getSize() > GCSService.FILE_SIZE) {
            lines = Arrays.copyOf(lines, lines.length - 1);
          }
          Assert.assertTrue(lines.length == lines.length - 1);
        } else {
          // write it as binary
          Assert.assertTrue(true);
        }
      }
    }
  }

  @Test
  @Ignore
  public void testFileName() throws Exception {
    String path = "demo/csv/titanic.csv";
    File file = new File(path);
    String name = file.getName();
    Assert.assertEquals("titanic.csv", name);
  }

  private Storage getStorage(String projectId, String path) throws Exception {
    ServiceAccountCredentials credentials = GCPServiceAccount.loadLocalFile(path);

    Storage storage = StorageOptions.newBuilder()
      .setProjectId(projectId)
      .setCredentials(credentials)
      .build()
      .getService();

    return storage;
  }

}