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
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

public class OperationTestBase extends AbstractValidatingStargateBridgeTest {
  protected final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  protected final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  protected final CommandContext CONTEXT = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  protected ColumnDefinitions buildColumnDefs(List<String> columnNames, List<Integer> typeCodes) {
    return buildColumnDefs(KEYSPACE_NAME, COLLECTION_NAME, columnNames, typeCodes);
  }

  protected ColumnDefinitions buildColumnDefs(
      String ks, String tableName, List<String> columnNames, List<Integer> typeCodes) {
    List<ColumnDefinition> columnDefs = new ArrayList<>();
    for (int ix = 0, end = columnNames.size(); ix < end; ++ix) {
      columnDefs.add(
          new DefaultColumnDefinition(
              new ColumnSpec(
                  ks,
                  tableName,
                  columnNames.get(ix),
                  ix,
                  RawType.PRIMITIVES.get(typeCodes.get(ix))),
              AttachmentPoint.NONE));
    }
    return DefaultColumnDefinitions.valueOf(columnDefs);
  }

  protected ByteBuffer byteBufferFrom(long value) {
    return TypeCodecs.BIGINT.encode(value, ProtocolVersion.DEFAULT);
  }
}
