package io.stargate.sgv2.jsonapi.service.shredding.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.mock.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class WriteableTableRowTest {

  @ParameterizedTest
  @MethodSource("missingPartitionKeysFixtures")
  public void missingPartitionKeys(WriteableTableRowFixture fixture) {

    assertThat(fixture.missingKeys())
        .as("Fixture has unsupported key columns, fixture=%s", fixture)
        .isNotEmpty();

    var tableSchemaObject = new TableSchemaObject(fixture.cqlFixture().tableMetadata());

    var e =
        assertThrowsExactly(
            DocumentException.class,
            () -> SchemaValidatable.maybeValidate(tableSchemaObject, fixture.row()),
            String.format(
                "Throw exception when row has missing primary keys using fixture=%s", fixture));

    assertThat(e.code)
        .as("Using correct error code")
        .isEqualTo(DocumentException.Code.MISSING_PRIMARY_KEY_COLUMNS.name());

    fixture
        .missingKeys()
        .forEach(
            missingKey -> {
              assertThat(e)
                  .as("Missing key column included in message %s", missingKey)
                  .hasMessageContaining(errFmt(missingKey));
            });
  }

  private static Stream<Arguments> missingPartitionKeysFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new WriteableTableRowFixtureSupplier.MissingPrimaryKeys(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });
    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("allColumnsFixtures")
  public void allColumns(WriteableTableRowFixture fixture) {

    assertThat(fixture.allMissingColumns())
        .as("Fixture has no missing columns, fixture=%s", fixture)
        .isEmpty();

    var tableSchemaObject = new TableSchemaObject(fixture.cqlFixture().tableMetadata());

    assertDoesNotThrow(
        () -> SchemaValidatable.maybeValidate(tableSchemaObject, fixture.row()),
        String.format("Valid to insert row with all columns set row=%s", fixture.row()));
  }

  private static Stream<Arguments> allColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new WriteableTableRowFixtureSupplier.AllColumns(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });

    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("missingNonKeyColumnsFixtures")
  public void missingNonKeyColumns(WriteableTableRowFixture fixture) {

    assertThat(fixture.missingNonKeyColumns())
        .as("Fixture has missing non key columns, fixture=%s", fixture)
        .isNotEmpty();

    var tableSchemaObject = new TableSchemaObject(fixture.cqlFixture().tableMetadata());

    assertDoesNotThrow(
        () -> SchemaValidatable.maybeValidate(tableSchemaObject, fixture.row()),
        String.format(
            "Valid to insert row with all PrimaryKeys and missing columns row=%s", fixture.row()));
  }

  private static Stream<Arguments> missingNonKeyColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new WriteableTableRowFixtureSupplier.MissingNonKeyColumns(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });

    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("unknownColumnsFixtures")
  public void unknownColumns(WriteableTableRowFixture fixture) {

    assertThat(fixture.unknownAllColumns())
        .as("Fixture has unknown columns, fixture=%s", fixture)
        .isNotEmpty();

    var tableSchemaObject = new TableSchemaObject(fixture.cqlFixture().tableMetadata());

    var e =
        assertThrowsExactly(
            DocumentException.class,
            () -> SchemaValidatable.maybeValidate(tableSchemaObject, fixture.row()),
            String.format(
                "Throw exception when row has missing primary keys using fixture=%s", fixture));

    assertThat(e.code)
        .as("Using correct error code")
        .isEqualTo(DocumentException.Code.UNKNOWN_TABLE_COLUMNS.name());

    fixture
        .unknownAllColumns()
        .forEach(
            unknonwnColumn -> {
              assertThat(e)
                  .as("Unknown column included in message %s", unknonwnColumn)
                  .hasMessageContaining(errFmt(unknonwnColumn));
            });
  }

  private static Stream<Arguments> unknownColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new WriteableTableRowFixtureSupplier.UnknownColumns(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });
    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("unsupportedColumnsFixtures")
  public void unsupportedColumns(WriteableTableRowFixture fixture) {

    assertThat(fixture.unsupportedAllColumns())
        .as("Fixture has unsupported columns, fixture=%s", fixture)
        .isNotEmpty();

    var tableSchemaObject = new TableSchemaObject(fixture.cqlFixture().tableMetadata());

    var e =
        assertThrowsExactly(
            DocumentException.class,
            () -> SchemaValidatable.maybeValidate(tableSchemaObject, fixture.row()),
            String.format(
                "Throw exception when row has unsupported column types fixture=%s", fixture));

    assertThat(e.code)
        .as("Using correct error code")
        .isEqualTo(DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.name());

    fixture
        .unsupportedAllColumns()
        .forEach(
            unknonwnColumn -> {
              assertThat(e)
                  .as("Unknown column included in message %s", unknonwnColumn)
                  .hasMessageContaining(errFmt(unknonwnColumn));
            });
  }

  private static Stream<Arguments> unsupportedColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    var cqlFixtures =
        CqlFixture.allFixtures(
            CqlIdentifiersSource.ALL_CLASSES, // all the different types of identifiers
            List.of(new CqlDataSource.UnsupportedData()), // need unsupported types
            List.of(new TableMetadataFixtureSource.AllUnsupportedTypes()) // need unsupported types;
            );

    cqlFixtures.forEach(
        cqlFixture -> {
          new WriteableTableRowFixtureSupplier.AllColumns(cqlFixture)
              .get()
              .forEach(fixture -> testCases.add(Arguments.of(fixture)));
        });
    return testCases.stream();
  }
}
