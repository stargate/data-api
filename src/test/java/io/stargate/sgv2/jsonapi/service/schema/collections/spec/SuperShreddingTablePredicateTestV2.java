package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;
import static io.stargate.sgv2.jsonapi.util.TableMetadataTestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.util.LoggerTestWrapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuperShreddingTablePredicateTestV2 extends SuperShreddingBuilderTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SuperShreddingTablePredicateTestV2.class);

  private void assertPredicate(
      String testName,
      boolean expectedResult,
      SuperShreddingPredicateBuilder predicateBuilder,
      SuperShreddingMetadataBuilder builder,
      String logMessage) {
    assertPredicate(
        testName,
        expectedResult,
        predicateBuilder.buildTableOnly(),
        (TableMetadata) builder.buildTableOnly(),
        logMessage);
  }

  private void assertPredicate(
      String testName,
      boolean expectedResult,
      SuperShreddingTablePredicate predicate,
      TableMetadata tableMetadata,
      String logMessage) {

    try (var logWrapper = new LoggerTestWrapper(SuperShreddingTablePredicate.class)) {

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "{} - expectedResult:{} , tableMetadata:{}",
            testName,
            expectedResult,
            tableMetadata == null ? "null" : tableMetadata.describe(true));
      }

      var predicateResult = predicate.test(tableMetadata);
      LOGGER.info(
          "{} - expectedResult:{}, predicateResult:{}", testName, expectedResult, predicateResult);
      assertThat(predicateResult)
          .as("%s - predicate is %s", testName, expectedResult)
          .isEqualTo(expectedResult);

      if (logMessage != null) {
        assertThat(logWrapper.logMessages())
            .as("%s - log message: %s", testName, logMessage)
            .anyMatch(s -> s.contains(logMessage));
      }
    }
  }

  @Test
  public void nullTableMetadata() {
    var predicate = configAllOptional(SuperShreddingPredicateBuilder.predicate()).buildTableOnly();

    assertPredicate("nullTableMetadata()", false, predicate, null, null);
  }

  @Test
  public void createTableAllOptional() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var predicateBuilder = configAllOptional(SuperShreddingPredicateBuilder.predicate());

    assertPredicate("createTableAllOptional()", true, predicateBuilder, metadataBuilder, null);
  }

  @Test
  public void createTableNoOptional() {

    var metadataBuilder = configNoOptional(SuperShreddingBuilder.metadata());
    var predicateBuilder = configNoOptional(SuperShreddingPredicateBuilder.predicate());
    assertPredicate("createTableNoOptional()", true, predicateBuilder, metadataBuilder, null);
  }

  @Test
  public void createTableVectorOnly() {

    var metadataBuilder = configVectorOnly(SuperShreddingBuilder.metadata());
    var predicateBuilder = configVectorOnly(SuperShreddingBuilder.predicate());
    assertPredicate("createTableVectorOnly()", true, predicateBuilder, metadataBuilder, null);
  }

  @Test
  public void createTableLexicalOnly() {

    var metadataBuilder = configLexicalOnly(SuperShreddingBuilder.metadata());
    var predicateBuilder = configLexicalOnly(SuperShreddingBuilder.predicate());
    assertPredicate("createTableLexicalOnly()", true, predicateBuilder, metadataBuilder, null);
  }

  @Test
  public void removeColumns() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var tableMetadata = (TableMetadata) metadataBuilder.buildTableOnly();
    var predicate = configAllOptional(SuperShreddingBuilder.predicate()).buildTableOnly();

    // we expect all columns to be present, so use that as the list
    removeAllColumns(tableMetadata, SuperShreddingMetadata.Identifiers.ALL)
        .forEach(
            entry -> {
              assertPredicate(
                  "removeColumns(%s)".formatted(entry.column()),
                  false,
                  predicate,
                  entry.tableMetadata(),
                  "columns missing, columns: " + cqlIdentifierToMessageString(entry.column()));
            });
  }

  @Test
  public void removePartitionKey() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var tableMetadata = (TableMetadata) metadataBuilder.buildTableOnly();
    var predicate = configAllOptional(SuperShreddingBuilder.predicate()).buildTableOnly();

    removeAllPartitionKeys(tableMetadata)
        .forEach(
            entry -> {
              assertPredicate(
                  "removePartitionKey(%s)".formatted(entry.column()),
                  false,
                  predicate,
                  entry.tableMetadata(),
                  "partition key missing, columns: "
                      + cqlIdentifierToMessageString(entry.column()));
            });
  }

  @Test
  public void swapColumnTypes() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var tableMetadata = (TableMetadata) metadataBuilder.buildTableOnly();
    var predicate = configAllOptional(SuperShreddingBuilder.predicate()).buildTableOnly();

    // we expect all columns to be present, so use that as the list
    swapTypesAllColumns(
            tableMetadata,
            SuperShreddingMetadata.Identifiers.ALL,
            DataTypes.TINYINT,
            DataTypes.TEXT)
        .forEach(
            entry -> {
              assertPredicate(
                  "swapColumnTypes(%s)".formatted(entry.column()),
                  false,
                  predicate,
                  entry.tableMetadata(),
                  "columns missing, columns: " + cqlIdentifierToMessageString(entry.column()));
            });
  }

  @Test
  public void unexpectedPartitionKeys() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var tableMetadata = (TableMetadata) metadataBuilder.buildTableOnly();

    var columnName = "unexpected_key";
    var updatedTableAppended = addPartitionKey(tableMetadata, false, columnName, DataTypes.TEXT);
    var updatedTableClearFirst = addPartitionKey(tableMetadata, true, columnName, DataTypes.TEXT);

    var predicate = configAllOptional(SuperShreddingBuilder.predicate()).buildTableOnly();

    assertPredicate(
        "unexpectedPartitionKeys(%s - %s)".formatted(columnName, "appended"),
        false,
        predicate,
        updatedTableAppended,
        "unexpected columns in partition key, columns: %s(%s)"
            .formatted(columnName, errFmt(DataTypes.TEXT)));

    // This is really the same as removing the key but testing for completeness
    assertPredicate(
        "unexpectedPartitionKeys(%s - %s)".formatted(columnName, "clearFirst"),
        false,
        predicate,
        updatedTableClearFirst,
        "partition key missing, columns: key");
  }

  @Test
  public void unexpectedClusteringColumns() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var tableMetadata = (TableMetadata) metadataBuilder.buildTableOnly();
    var columnName = "unexpected_column";
    var updatedTable = addClusteringColumn(tableMetadata, columnName, DataTypes.TEXT);

    var predicate = configAllOptional(SuperShreddingBuilder.predicate()).buildTableOnly();

    assertPredicate(
        "unexpectedClusteringColumns(%s)".formatted(columnName),
        false,
        predicate,
        updatedTable,
        "unexpected columns in clustering key, columns: %s(%s)"
            .formatted(columnName, errFmt(DataTypes.TEXT)));
  }

  @Test
  public void unexpectedColumnsStrictMode() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var tableMetadata = (TableMetadata) metadataBuilder.buildTableOnly();
    var columnName = "unexpected_column";
    var updatedTable = addColumn(tableMetadata, columnName, DataTypes.TEXT);

    var predicate = configAllOptional(SuperShreddingBuilder.predicate()).buildTableOnly();

    assertPredicate(
        "unexpectedColumnsStrictMode(%s)".formatted(columnName),
        false,
        predicate,
        updatedTable,
        "unexpected columns in strict mode, columns: unexpected_column(text)"
            .formatted(columnName, errFmt(DataTypes.TEXT)));
  }

  @Test
  public void unexpectedColumnsRelaxedMode() {

    var metadataBuilder = configAllOptional(SuperShreddingBuilder.metadata());
    var tableMetadata = (TableMetadata) metadataBuilder.buildTableOnly();
    var columnName = "unexpected_column";
    var updatedTable = addColumn(tableMetadata, columnName, DataTypes.TEXT);

    var predicate =
        configAllOptional(SuperShreddingBuilder.predicate()).withStrict(false).buildTableOnly();

    // in non-strict mode, we can have an extra column
    assertPredicate(
        "unexpectedColumnsRelaxedMode(%s)".formatted(columnName),
        true,
        predicate,
        updatedTable,
        null);
  }
}
