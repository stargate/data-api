package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

public class OperationTestBase {
  protected final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  protected final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  protected final CommandContext CONTEXT = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  protected ColumnDefinitions buildColumnDefs(List<TestColumn> columns) {
    return buildColumnDefs(KEYSPACE_NAME, COLLECTION_NAME, columns);
  }

  protected ColumnDefinitions buildColumnDefs(
      String ks, String tableName, List<TestColumn> columns) {
    List<ColumnDefinition> columnDefs = new ArrayList<>();
    for (int ix = 0, end = columns.size(); ix < end; ++ix) {
      columnDefs.add(
          new DefaultColumnDefinition(
              new ColumnSpec(
                  ks,
                  tableName,
                  columns.get(ix).name(),
                  ix,
                  RawType.PRIMITIVES.get(columns.get(ix).type())),
              AttachmentPoint.NONE));
    }
    return DefaultColumnDefinitions.valueOf(columnDefs);
  }

  protected ByteBuffer byteBufferFrom(long value) {
    return TypeCodecs.BIGINT.encode(value, ProtocolVersion.DEFAULT);
  }

  protected record TestColumn(String name, int type) {
    static TestColumn of(String name, int type) {
      return new TestColumn(name, type);
    }
  }
}
