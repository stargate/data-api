package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.fixtures.data.FixtureData;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base for classes that build a test data scenario */
public abstract class TestDataScenario {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestDataScenario.class);
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  public static final ApiColumnDef ID_COL =
      new ApiColumnDef(CqlIdentifier.fromInternal("id"), ApiDataTypeDefs.TEXT);

  protected final String keyspaceName;
  protected final String tableName;

  public final FixtureData dataSource;
  public final ApiColumnDef primaryKey;
  public final List<ApiClusteringDef> clusteringDefs;
  public final ApiColumnDefContainer allColumns;
  public final ApiColumnDefContainer nonPkColumns;

  protected TestDataScenario(
      String keyspaceName,
      String tableName,
      ApiColumnDef primaryKey,
      List<ApiClusteringDef> clusteringDefs,
      ApiColumnDefContainer allColumns,
      FixtureData dataSource) {
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
    this.dataSource = dataSource;

    this.primaryKey = primaryKey;
    this.clusteringDefs =
        clusteringDefs == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(clusteringDefs);
    this.allColumns = allColumns.toUnmodifiable();
    var workingNonPkColumns = new ApiColumnDefContainer(allColumns);
    workingNonPkColumns.remove(primaryKey.name());
    this.nonPkColumns = workingNonPkColumns.toUnmodifiable();
  }

  public void create() {
    createTable();
    createIndexes();
    insertRows();
  }

  public void drop() {
    assertNamespaceCommand(keyspaceName).templated().dropTable(tableName, false).wasSuccessful();
  }

  public static String fieldName(ApiColumnDef apiColumnDef) {
    return apiColumnDef.name().asInternal();
  }

  public Object columnValue(ApiColumnDef apiColumnDef) {
    var jsonLiteral = dataSource.fromJSON(apiColumnDef);
    if (jsonLiteral.type() == JsonType.ARRAY) {
      List<JsonLiteral<?>> literals = (List<JsonLiteral<?>>) jsonLiteral.value();
      return literals.stream().map(JsonLiteral::value).toArray();
    }
    return jsonLiteral.value();
  }

  protected void createTable() {
    createTableWithTypes(keyspaceName, tableName, primaryKey, clusteringDefs, allColumns);
  }

  protected void createIndexes() {}
  ;

  protected void insertRows() {}
  ;

  // ========= Helper methods =========

  protected void createTableWithTypes(
      String keyspaceName,
      String tableName,
      ApiColumnDef primaryKey,
      List<ApiClusteringDef> clusteringDefs,
      ApiColumnDefContainer columns) {

    LOGGER.warn("Creating table {}.{} with columns {}", keyspaceName, tableName, columns);
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(tableName, columns, ApiColumnDefContainer.of(primaryKey), clusteringDefs)
        .wasSuccessful();
  }

  protected void insertManyRows(List<Map<String, Object>> rows) {
    if (LOGGER.isWarnEnabled()) {
      for (var row : rows) {
        LOGGER.warn("Inserting row {}", row);
      }
    }
    assertTableCommand(keyspaceName, tableName).templated().insertManyMap(rows).wasSuccessful();
  }

  protected static void addColumnsForTypes(
      ApiColumnDefContainer columns, List<PrimitiveApiDataTypeDef> types, String columnPrefix) {
    addColumnsForTypes(
        columns,
        types,
        type -> CqlIdentifier.fromInternal(columnPrefix + type.typeName().apiName()));
  }

  protected static void addColumnsForTypes(
      ApiColumnDefContainer columns,
      List<PrimitiveApiDataTypeDef> types,
      Function<ApiDataType, CqlIdentifier> nameMapper) {
    types.forEach(
        type -> {
          columns.put(new ApiColumnDef(nameMapper.apply(type), type));
        });
  }

  protected void addNonPKColValuesWithNull(Map<String, Object> row, int nullIndex) {

    var i = 0;
    for (var apiColumnDef : nonPkColumns.values()) {
      row.put(fieldName(apiColumnDef), i == nullIndex ? null : columnValue(apiColumnDef));
      i++;
    }
  }

  protected List<Map<String, Object>> createRowForEachNonPkColumnWithNull(
      Function<ApiColumnDef, Object> pkValueSupplier) {
    var rows = new ArrayList<Map<String, Object>>();

    // and a row with all the columns set
    var row = new HashMap<String, Object>();
    row.put(fieldName(primaryKey), pkValueSupplier.apply(primaryKey));
    addNonPKColValuesWithNull(row, -1);
    rows.add(row);

    var i = 0;
    for (var apiColumnDef : nonPkColumns.values()) {
      row = new HashMap<String, Object>();
      row.put(fieldName(primaryKey), pkValueSupplier.apply(primaryKey));
      addNonPKColValuesWithNull(row, i++);
      rows.add(row);
    }

    return rows;
  }
}
