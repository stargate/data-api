package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Projectable;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableProjection;
import io.stargate.sgv2.jsonapi.service.processor.CommandContextTestData;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.UUID;
import org.mockito.Mockito;

/** Test fixture for TableProjection unit tests, following fluent style used elsewhere. */
public class TableProjectionTestData extends TestDataSuplier {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public TableProjectionTestData(TestData testData) {
    super(testData);
  }

  public Fixture tableWithAllDataTypes(String message) {
    var tm = testData.tableMetadata().tableAllDatatypesNotIndexed();
    return new Fixture(message, tm, TableSchemaObject.from(tm, new ObjectMapper()));
  }

  public Fixture tableWithAllDataTypesPlusUdt(String message) {
    var tm = testData.tableMetadata().tableAllDatatypesNotIndexed();
    return new Fixture(message, tm, TableSchemaObject.from(tm, new ObjectMapper()));
  }

  public static class Fixture implements Recordable {
    public final String message;
    public final TableMetadata tableMetadata;
    public final TableSchemaObject tableSchemaObject;

    private TableProjection projection;
    private String appliedCql;

    public Fixture(
        String message, TableMetadata tableMetadata, TableSchemaObject tableSchemaObject) {
      this.message = message;
      this.tableMetadata = tableMetadata;
      this.tableSchemaObject = tableSchemaObject;
    }

    public Fixture withProjectionJson(String json) {
      try {
        Projectable cmd =
            new Projectable() {
              private final JsonNode def = OBJECT_MAPPER.readTree(json);

              @Override
              public JsonNode projectionDefinition() {
                return def;
              }
            };

        CommandContext<TableSchemaObject> ctx =
            new CommandContextTestData(new TestData())
                .tableSchemaObjectCommandContext(tableSchemaObject);
        this.projection = TableProjection.fromDefinition(ctx, OBJECT_MAPPER, cmd);
        return this;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public Fixture applySelect() {
      var select = new DefaultSelect(tableMetadata.getKeyspace(), tableMetadata.getName()).all();
      this.appliedCql = projection.apply(select).asCql();
      return this;
    }

    public Fixture assertSelectContains(String... fragments) {
      for (String f : fragments) {
        assertThat(appliedCql).as("%s contains %s", message, f).contains(f);
      }
      return this;
    }

    public Fixture assertSelectNotContains(String... fragments) {
      for (String f : fragments) {
        assertThat(appliedCql).as("%s not contains %s", message, f).doesNotContain(f);
      }
      return this;
    }

    /**
     * Project a row with all necessary selected columns populated with default values. This method
     * internally creates a complete row with all selected columns and then projects it.
     */
    public JsonNode projectRow() {
      var selectedColumns = projection.getSelectedColumns();
      Object[] allValues = new Object[selectedColumns.size()];

      // Fill with default values based on column types
      for (int i = 0; i < selectedColumns.size(); i++) {
        var column = selectedColumns.get(i);
        allValues[i] = getDefaultValueForColumn(column);
      }

      // Create a mock row that returns the values in order
      Row row = Mockito.mock(Row.class);
      for (int i = 0; i < allValues.length; i++) {
        final int index = i;
        Mockito.when(row.getObject(index)).thenReturn(allValues[index]);
      }

      return projection.projectRow(row);
    }

    /**
     * Project a row with custom values for specific columns. This method allows overriding default
     * values for testing specific scenarios.
     *
     * @param overrides Map of column names to custom values
     * @return projected JSON result
     */
    public JsonNode projectRowWithOverrides(Map<String, Object> overrides) {
      var selectedColumns = projection.getSelectedColumns();
      Object[] allValues = new Object[selectedColumns.size()];

      // Fill with default values, then apply overrides
      for (int i = 0; i < selectedColumns.size(); i++) {
        var column = selectedColumns.get(i);
        var columnName = column.getName().asInternal();

        // Use override value if provided, otherwise use default
        allValues[i] = overrides.getOrDefault(columnName, getDefaultValueForColumn(column));
      }

      // Create a mock row that returns the values in order
      Row row = Mockito.mock(Row.class);
      for (int i = 0; i < allValues.length; i++) {
        final int index = i;
        Mockito.when(row.getObject(index)).thenReturn(allValues[index]);
      }

      return projection.projectRow(row);
    }

    /** Get a default value for a column based on its type. */
    private Object getDefaultValueForColumn(ColumnMetadata column) {
      var type = column.getType();
      if (type == DataTypes.TEXT) {
        return "default_text";
      } else if (type == DataTypes.INT) {
        return 42;
      } else if (type == DataTypes.BOOLEAN) {
        return true;
      } else if (type == DataTypes.DOUBLE) {
        return 3.14;
      } else if (type == DataTypes.FLOAT) {
        return 2.5f;
      } else if (type == DataTypes.BIGINT) {
        return 123456789L;
      } else if (type == DataTypes.UUID) {
        return UUID.randomUUID();
      } else if (type == DataTypes.DATE) {
        return LocalDate.now();
      } else if (type == DataTypes.TIMESTAMP) {
        return Instant.now();
      } else if (type == DataTypes.TIME) {
        return LocalTime.now();
      } else if (type == DataTypes.DECIMAL) {
        return BigDecimal.valueOf(123.45);
      } else if (type instanceof UserDefinedType) {
        // For UDT, create a default UdtValue
        var udtType = (UserDefinedType) type;
        var udtValue = udtType.newValue();
        // Set default values for UDT fields
        for (var fieldName : udtType.getFieldNames()) {
          if (udtType.getFieldTypes().get(udtType.getFieldNames().indexOf(fieldName))
              == DataTypes.TEXT) {
            // Use the actual field name as generated by TestDataNames (includes timestamp)
            udtValue = udtValue.set(fieldName, "default_" + fieldName.asInternal(), String.class);
          }
        }
        return udtValue;
      }
      // For other types, return null
      return null;
    }

    public Fixture assertJsonHasInt(JsonNode node, CqlIdentifier key, int value) {
      assertThat(node.get(key.asInternal()).asInt()).isEqualTo(value);
      return this;
    }

    public Fixture assertJsonNodeSize(JsonNode node, int expectedSize) {
      assertThat(node.size()).isEqualTo(expectedSize);
      return this;
    }

    public Fixture assertJsonMissing(JsonNode node, CqlIdentifier... keys) {
      for (var k : keys) {
        assertThat(node.get(k.asInternal())).isNull();
      }
      return this;
    }

    public Fixture assertJsonHasObject(JsonNode node, CqlIdentifier key) {
      assertThat(node.get(key.asInternal())).isNotNull();
      return this;
    }

    public Fixture assertJsonHasString(JsonNode node, CqlIdentifier key, String value) {
      assertThat(node.get(key.asInternal()).asText()).isEqualTo(value);
      return this;
    }

    public Fixture assertJsonHasBoolean(JsonNode node, CqlIdentifier key, boolean value) {
      assertThat(node.get(key.asInternal()).asBoolean()).isEqualTo(value);
      return this;
    }

    public Fixture assertJsonHasDouble(JsonNode node, CqlIdentifier key, double value) {
      assertThat(node.get(key.asInternal()).asDouble()).isEqualTo(value);
      return this;
    }

    public Fixture assertJsonHasLong(JsonNode node, CqlIdentifier key, long value) {
      assertThat(node.get(key.asInternal()).asLong()).isEqualTo(value);
      return this;
    }

    public Fixture assertJsonHasBigDecimal(JsonNode node, CqlIdentifier key, BigDecimal value) {
      assertThat(node.get(key.asInternal()).decimalValue()).isEqualTo(value);
      return this;
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("message", message).append("table", tableMetadata.describe(true));
    }
  }
}
