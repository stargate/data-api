package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
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
      TestColumn column = columns.get(ix);
      columnDefs.add(
          new DefaultColumnDefinition(
              new ColumnSpec(ks, tableName, column.name(), ix, column.type()),
              AttachmentPoint.NONE));
    }
    return DefaultColumnDefinitions.valueOf(columnDefs);
  }

  protected ByteBuffer byteBufferFrom(long value) {
    return TypeCodecs.BIGINT.encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFrom(boolean value) {
    return TypeCodecs.BOOLEAN.encode(value, ProtocolVersion.DEFAULT);
  }

  private static TupleType keyTupleType = DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT);

  protected ByteBuffer byteBufferFrom(TupleValue value) {
    return TypeCodecs.tupleOf(keyTupleType).encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFrom(String value) {
    return TypeCodecs.TEXT.encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFrom(UUID value) {
    return TypeCodecs.UUID.encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferForKey(String key) {
    TupleType tupleType = DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT);
    // important! TINYINT means Byte
    TupleValue value = tupleType.newValue((byte) DocumentConstants.KeyTypeId.TYPE_ID_STRING, key);
    return TypeCodecs.tupleOf(tupleType).encode(value, ProtocolVersion.DEFAULT);
  }

  protected record TestColumn(String name, RawType type) {
    static TestColumn of(String name, int typeId) {
      return new TestColumn(name, RawType.PRIMITIVES.get(typeId));
    }

    static TestColumn of(String name, RawType type) {
      return new TestColumn(name, type);
    }

    static TestColumn ofLong(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.BIGINT));
    }

    static TestColumn ofVarchar(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR));
    }

    static TestColumn ofUuid(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.UUID));
    }

    static TestColumn ofBoolean(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.BOOLEAN));
    }

    static TestColumn keyColumn() {
      List<RawType> keyTupleParams =
          Arrays.asList(
              RawType.PRIMITIVES.get(ProtocolConstants.DataType.TINYINT),
              RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR));
      return of("key", new RawType.RawTuple(keyTupleParams));
    }
  }
}
