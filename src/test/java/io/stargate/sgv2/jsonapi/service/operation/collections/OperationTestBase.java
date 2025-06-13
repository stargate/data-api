package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.data.CqlVector;
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
import io.quarkus.test.InjectMock;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObjectName;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;

public class OperationTestBase {

  // aaron feb 11 2025 - not sure how this is working, not sure how @Inject works when then
  // super class is not under quarkus control. But if I remove it tests break
  @Inject JsonProcessingMetricsReporter jsonProcessingMetricsReporter;

  // this will work even though the base class is not managed by Quarkus
  @InjectMock protected RequestContext dataApiRequestInfo;

  private final TestConstants testConstants = new TestConstants();

  protected final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  protected final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  protected final SchemaObjectName SCHEMA_OBJECT_NAME =
      new SchemaObjectName(KEYSPACE_NAME, COLLECTION_NAME);

  protected CollectionSchemaObject COLLECTION_SCHEMA_OBJECT;
  protected KeyspaceSchemaObject KEYSPACE_SCHEMA_OBJECT;

  protected CommandContext<CollectionSchemaObject> COLLECTION_CONTEXT;
  protected CommandContext<KeyspaceSchemaObject> KEYSPACE_CONTEXT;

  protected static final TupleType DOC_KEY_TYPE =
      DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT);

  @BeforeEach
  public void beforeEach() {
    // must do this here to avoid touching quarkus config before it is initialized
    COLLECTION_SCHEMA_OBJECT =
        new CollectionSchemaObject(
            SCHEMA_OBJECT_NAME,
            null,
            IdConfig.defaultIdConfig(),
            VectorConfig.NOT_ENABLED_CONFIG,
            null,
            CollectionLexicalConfig.configForDisabled(),
            CollectionRerankDef.configForPreRerankingCollection());

    KEYSPACE_SCHEMA_OBJECT = KeyspaceSchemaObject.fromSchemaObject(COLLECTION_SCHEMA_OBJECT);

    COLLECTION_CONTEXT =
        testConstants.collectionContext(
            testConstants.TEST_COMMAND_NAME,
            COLLECTION_SCHEMA_OBJECT,
            jsonProcessingMetricsReporter,
            null);
    KEYSPACE_CONTEXT =
        testConstants.keyspaceContext(
            testConstants.TEST_COMMAND_NAME, KEYSPACE_SCHEMA_OBJECT, jsonProcessingMetricsReporter);
  }

  protected CommandContext<CollectionSchemaObject> createCommandContextWithCommandName(
      String commandName) {
    return testConstants.collectionContext(
        commandName, COLLECTION_SCHEMA_OBJECT, jsonProcessingMetricsReporter, null);
  }

  protected ColumnDefinitions buildColumnDefs(TestColumn... columns) {
    return buildColumnDefs(Arrays.asList(columns));
  }

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

  protected ByteBuffer byteBufferFrom(boolean value) {
    return TypeCodecs.BOOLEAN.encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFrom(long value) {
    return TypeCodecs.BIGINT.encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFrom(TupleValue value) {
    return TypeCodecs.tupleOf(DOC_KEY_TYPE).encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFrom(String value) {
    return TypeCodecs.TEXT.encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFrom(UUID value) {
    return TypeCodecs.UUID.encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferFromAny(Object value) {
    if (value == null) {
      return byteBufferForNull();
    }
    if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    }
    if (value instanceof Boolean) {
      return byteBufferFrom((Boolean) value);
    }
    if (value instanceof UUID) {
      return byteBufferFrom((UUID) value);
    }
    if (value instanceof String) {
      return byteBufferFrom((String) value);
    }
    if (value instanceof Long) {
      return byteBufferFrom((Long) value);
    }
    throw new IllegalArgumentException(
        "byteBufferFromAny() does not (yet?) support value of type: " + value.getClass());
  }

  protected ByteBuffer byteBufferForKey(String key) {
    // Important! TINYINT requires Byte:
    TupleValue value =
        DOC_KEY_TYPE.newValue((byte) DocumentConstants.KeyTypeId.TYPE_ID_STRING, key);
    return TypeCodecs.tupleOf(DOC_KEY_TYPE).encode(value, ProtocolVersion.DEFAULT);
  }

  protected ByteBuffer byteBufferForNull() {
    return ByteBuffer.wrap(new byte[0]);
  }

  /**
   * Factory method for constructing value for Document key of type {@code String}, to be used for
   * {@code SimpleStatement} bound values.
   *
   * @param key String id of the document
   * @return Bound value to use with {@code SimpleStatement}
   */
  protected TupleValue boundKeyForStatement(String key) {
    return CQLBindValues.getDocumentIdValue(DocumentId.fromString(key));
  }

  protected CqlVector<Float> vectorForStatement(Float... value) {
    return CqlVector.newInstance(value);
  }

  protected record TestColumn(String name, RawType type) {
    static TestColumn of(String name, int typeId) {
      return new TestColumn(name, RawType.PRIMITIVES.get(typeId));
    }

    static TestColumn of(String name, RawType type) {
      return new TestColumn(name, type);
    }

    static TestColumn ofBoolean(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.BOOLEAN));
    }

    static TestColumn ofDate(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.DATE));
    }

    static TestColumn ofDecimal(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.DECIMAL));
    }

    static TestColumn ofLong(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.BIGINT));
    }

    static TestColumn ofVarchar(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR));
    }

    static TestColumn ofTimestamp(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.TIMESTAMP));
    }

    static TestColumn ofUuid(String name) {
      return of(name, RawType.PRIMITIVES.get(ProtocolConstants.DataType.UUID));
    }

    static TestColumn keyColumn() {
      return of(
          "key",
          new RawType.RawTuple(
              Arrays.asList(
                  RawType.PRIMITIVES.get(ProtocolConstants.DataType.TINYINT),
                  RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))));
    }
  }
}
