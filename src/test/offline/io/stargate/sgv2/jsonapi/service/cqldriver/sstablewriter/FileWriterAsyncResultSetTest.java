package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class FileWriterAsyncResultSetTest {
  @Test
  public void testFileWriterAsyncResultSetContracts() {
    ColumnDefinitions columnDefs = mock(ColumnDefinitions.class);
    Row insertResponseRow = mock(Row.class);
    FileWriterAsyncResultSet fileWriterAsyncResultSet =
        new FileWriterAsyncResultSet(columnDefs, insertResponseRow);
    verifyAllContracts(fileWriterAsyncResultSet);
  }

  @Test
  public void testFileWriterAsyncResultSetWasApplied()
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    ColumnDefinitions columnDefs = buildColDefs("keyspace", "table");
    List<ByteBuffer> buffers = new ArrayList<>();
    buffers.add(TypeCodecs.BOOLEAN.encode(Boolean.TRUE, ProtocolVersion.DEFAULT));
    Row insertResponseRow = new FileWriterResponseRow(columnDefs, 0, buffers);
    FileWriterAsyncResultSet fileWriterAsyncResultSet =
        new FileWriterAsyncResultSet(columnDefs, insertResponseRow);
    verifyAllContracts(fileWriterAsyncResultSet);
    assertTrue(fileWriterAsyncResultSet.wasApplied());
  }

  @Test
  public void testFileWriterAsyncResultSetWasNotApplied()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ColumnDefinitions columnDefs = buildColDefs("keyspace", "table");
    List<ByteBuffer> buffers = new ArrayList<>();
    buffers.add(TypeCodecs.BOOLEAN.encode(Boolean.FALSE, ProtocolVersion.DEFAULT));
    Row insertResponseRow = new FileWriterResponseRow(columnDefs, 0, buffers);
    FileWriterAsyncResultSet fileWriterAsyncResultSet =
        new FileWriterAsyncResultSet(columnDefs, insertResponseRow);
    verifyAllContracts(fileWriterAsyncResultSet);
    assertFalse(fileWriterAsyncResultSet.wasApplied());
  }

  private ColumnDefinitions buildColDefs(String keyspace, String table)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method colDefsMethod =
        FileWriterSession.class.getDeclaredMethod(
            "getOfflineWriterResponseRowCols", String.class, String.class);
    colDefsMethod.setAccessible(true);
    FileWriterSession fileWriterSession = mock(FileWriterSession.class);
    return (ColumnDefinitions) colDefsMethod.invoke(fileWriterSession, keyspace, table);
  }

  private void verifyAllContracts(FileWriterAsyncResultSet fileWriterAsyncResultSet) {
    assertThrows(UnsupportedOperationException.class, fileWriterAsyncResultSet::getExecutionInfo);
    assertThrows(UnsupportedOperationException.class, fileWriterAsyncResultSet::remaining);
    assertFalse(fileWriterAsyncResultSet.hasMorePages());
    assertNull(fileWriterAsyncResultSet.fetchNextPage());
  }

  @Test
  public void testCoumnDefs()
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    ColumnDefinitions columnDefinitions = buildColDefs("keyspace", "table");
    assertEquals(1, columnDefinitions.size());
    assertEquals("[applied]", columnDefinitions.get(0).getName().toString());
    assertEquals("keyspace", columnDefinitions.get(0).getKeyspace().toString());
    assertEquals("table", columnDefinitions.get(0).getTable().toString());
    assertEquals(
        ProtocolConstants.DataType.BOOLEAN, columnDefinitions.get(0).getType().getProtocolCode());
  }
}
