package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.junit.jupiter.api.Test;

public class FileWriterSessionTest {
  @Test
  public void testInit()
      throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException,
          InvocationTargetException {
    CQLSessionCache cqlSessionCache = mock(CQLSessionCache.class);
    CQLSessionCache.SessionCacheKey cacheKey = null;
    String sessionId = UUID.randomUUID().toString();
    String keyspaceName = "keyspaceName";
    String tableName = "tableName";
    String ssTableOutputDirectory = System.getProperty("java.io.tmpdir") + "/sstables";
    if (!new File(ssTableOutputDirectory).exists()) {
      if (!new File(ssTableOutputDirectory).mkdirs()) {
        throw new IOException(
            "Unable to setup test. Failed to create directory: " + ssTableOutputDirectory);
      }
    }
    FileWriterParams fileWriterParams =
        getFileWriterParams(keyspaceName, tableName, ssTableOutputDirectory);
    FileWriterSession fileWritterSession =
        new FileWriterSession(cqlSessionCache, cacheKey, sessionId, fileWriterParams);
    Field cqlsSSTableWriterField = FileWriterSession.class.getDeclaredField("cqlsSSTableWriter");
    cqlsSSTableWriterField.setAccessible(true);
    CQLSSTableWriter cqlsSSTableWriter =
        (CQLSSTableWriter) cqlsSSTableWriterField.get(fileWritterSession);
    Method getEmtpyCassandraDirMethod =
        FileWriterSession.class.getDeclaredMethod("getEmptyCassandraDir");
    getEmtpyCassandraDirMethod.setAccessible(true);
    String emptyCassandraDataDirectory =
        getEmtpyCassandraDirMethod.invoke(fileWritterSession).toString();
    assertNotNull(cqlsSSTableWriter);
    assertTrue(new File(emptyCassandraDataDirectory).exists());
    assertEquals(sessionId, fileWritterSession.getName());
    assertThrows(UnsupportedOperationException.class, fileWritterSession::getMetadata);
    assertThrows(UnsupportedOperationException.class, fileWritterSession::isSchemaMetadataEnabled);
    // check if setSchemaMetadataEnabled throws UnsupportedOperationException
    assertThrows(
        UnsupportedOperationException.class,
        () -> fileWritterSession.setSchemaMetadataEnabled(true));
    assertThrows(UnsupportedOperationException.class, fileWritterSession::refreshSchemaAsync);
    assertThrows(
        UnsupportedOperationException.class, fileWritterSession::checkSchemaAgreementAsync);
    assertThrows(UnsupportedOperationException.class, fileWritterSession::getContext);
    assertEquals(keyspaceName.toLowerCase(), fileWritterSession.getKeyspace().get().toString());
    assertThrows(UnsupportedOperationException.class, fileWritterSession::getMetrics);
    assertEquals(keyspaceName, fileWritterSession.getNamespace());
    assertEquals(tableName, fileWritterSession.getCollection());
    assertEquals(keyspaceName, fileWritterSession.getStatus().keyspace());
    assertEquals(tableName, fileWritterSession.getStatus().tableName());
    assertEquals(ssTableOutputDirectory, fileWritterSession.getStatus().ssTableOutputDirectory());
    assertEquals(10, fileWritterSession.getStatus().fileWriterBufferSizeInMB());
    assertEquals(0, fileWritterSession.getStatus().dataDirectorySizeInMB());
    assertEquals(0, fileWritterSession.getStatus().insertsSucceeded());
    assertEquals(0, fileWritterSession.getStatus().insertsFailed());
  }

  private static @NonNull FileWriterParams getFileWriterParams(
      String keyspaceName, String tableName, String ssTableOutputDirectory) {
    int fileWriterBufferSizeInMB = 10;
    String createTableCQL =
        "CREATE TABLE IF NOT EXISTS keyspaceName.tableName (id UUID PRIMARY KEY, value TEXT);";
    String insertStatementCQL = "INSERT INTO keyspaceName.tableName (id, value) VALUES (?, ?);";
    List<String> indexCQLs =
        List.of(
            "CREATE INDEX IF NOT EXISTS idx_tableName_value ON keyspaceName.tableName (value);");
    Boolean vectorEnabled = true;
    return new FileWriterParams(
        keyspaceName,
        tableName,
        ssTableOutputDirectory,
        fileWriterBufferSizeInMB,
        createTableCQL,
        insertStatementCQL,
        indexCQLs,
        vectorEnabled);
  }
}
