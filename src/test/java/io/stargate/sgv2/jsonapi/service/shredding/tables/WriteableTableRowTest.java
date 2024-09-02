package io.stargate.sgv2.jsonapi.service.shredding.tables;

import static io.stargate.sgv2.jsonapi.exception.playing.ErrorFormatters.errFmt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import io.stargate.sgv2.jsonapi.exception.playing.DocumentException;
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

    var tableSchemaObject = new TableSchemaObject(fixture.cqlFixture().tableMetadata());

    var e =
        assertThrowsExactly(
            DocumentException.class,
            () -> SchemaValidatable.maybeValidate(tableSchemaObject, fixture.row()),
            String.format(
                "Throw exception when row has missing primary keys using fixture", fixture));

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
  @MethodSource("missingColumnsFixtures")
  public void missingColumns(WriteableTableRowFixture fixture) {

    var tableSchemaObject = new TableSchemaObject(fixture.cqlFixture().tableMetadata());

    assertDoesNotThrow(
        () -> SchemaValidatable.maybeValidate(tableSchemaObject, fixture.row()),
        String.format(
            "Valid to insert row with all PrimaryKeys and missing columns row=%s", fixture.row()));
  }

  private static Stream<Arguments> missingColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new WriteableTableRowFixtureSupplier.MissingColumns(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });

    return testCases.stream();
  }
}
