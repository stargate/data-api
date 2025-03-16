package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;

import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.fixtures.*;
import io.stargate.sgv2.jsonapi.fixtures.containers.json.*;
import io.stargate.sgv2.jsonapi.fixtures.data.OverflowData;
import io.stargate.sgv2.jsonapi.fixtures.data.UnderflowData;
import io.stargate.sgv2.jsonapi.fixtures.data.UnsupportedTypesData;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.BaseFixtureIdentifiers;
import io.stargate.sgv2.jsonapi.fixtures.tables.AllOverflowTypes;
import io.stargate.sgv2.jsonapi.fixtures.tables.AllUnderflowTypes;
import io.stargate.sgv2.jsonapi.fixtures.tables.AllUnsupportedTypes;
import io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteableTableRowBuilderTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteableTableRowBuilderTest.class);

  private static void logFixture(String testName, JsonContainerFixture fixture) {
    // 24-Jan-2025, tatu: This produces thousands of lines noise in logs, so let's
    //    change to TRACE level (from INFO)
    LOGGER.trace(
        "{}: \nfixture={} \ncontainer={} \ntable={}",
        testName,
        fixture.toString(true),
        PrettyPrintable.pprint(fixture.container()),
        fixture.cqlFixture().tableMetadata().describe(true));
  }

  private static WriteableTableRow buildRow(JsonContainerFixture fixture) {

    var commandContext =
        CommandContext.builderSupplier()
            .withJsonProcessingMetricsReporter(mock(JsonProcessingMetricsReporter.class))
            .withCqlSessionCache(mock(CQLSessionCache.class))
            .withCommandConfig(new CommandConfig())
            .withEmbeddingProviderFactory(mock(EmbeddingProviderFactory.class))
            .getBuilder(fixture.cqlFixture().tableSchemaObject())
            .withEmbeddingProvider(mock(EmbeddingProvider.class))
            .withCommandName("testCommand")
            .withRequestContext(new RequestContext(Optional.of("test-tenant")))
            .build();

    var builder =
        new WriteableTableRowBuilder(commandContext, JSONCodecRegistries.DEFAULT_REGISTRY);
    var row = builder.build(fixture.container());
    LOGGER.info("buildRow: row={}", PrettyPrintable.pprint(row));
    return row;
  }

  @ParameterizedTest
  @MethodSource("missingPartitionKeysFixtures")
  public void missingPartitionKeys(JsonContainerFixture fixture) {

    logFixture("missingPartitionKeys", fixture);

    assertThat(fixture.missingKeys())
        .as("Fixture has unsupported key columns, fixture=%s", fixture)
        .isNotEmpty();

    var e =
        assertThrowsExactly(
            DocumentException.class,
            () -> buildRow(fixture),
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
              new MissingPrimaryKeys(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });
    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("allColumnsFixtures")
  public void allColumns(JsonContainerFixture fixture) {

    logFixture("allColumns", fixture);
    assertThat(fixture.allMissingColumns())
        .as("Fixture has no missing columns, fixture=%s", fixture)
        .isEmpty();

    assertDoesNotThrow(
        () -> buildRow(fixture),
        String.format(
            "Valid to insert row with all columns set container=%s", fixture.container()));
  }

  private static Stream<Arguments> allColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new AllColumns(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });

    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("missingNonKeyColumnsFixtures")
  public void missingNonKeyColumns(JsonContainerFixture fixture) {

    logFixture("missingNonKeyColumns", fixture);
    assertThat(fixture.missingNonKeyColumns())
        .as("Fixture has missing non key columns, fixture=%s", fixture)
        .isNotEmpty();

    assertDoesNotThrow(
        () -> buildRow(fixture),
        String.format(
            "Valid to insert row with all PrimaryKeys and missing columns container=%s",
            fixture.container()));
  }

  private static Stream<Arguments> missingNonKeyColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new MissingNonKeyColumns(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });

    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("unknownColumnsFixtures")
  public void unknownColumns(JsonContainerFixture fixture) {

    logFixture("unknownColumns", fixture);
    assertThat(fixture.unknownAllColumns())
        .as("Fixture has unknown columns, fixture=%s", fixture)
        .isNotEmpty();

    var e =
        assertThrowsExactly(
            DocumentException.class,
            () -> buildRow(fixture),
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
                  .hasMessageContaining(errFmt(unknonwnColumn.getName()));
            });
  }

  private static Stream<Arguments> unknownColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    CqlFixture.allFixtures()
        .forEach(
            cqlFixture -> {
              new UnknownColumns(cqlFixture)
                  .get()
                  .forEach(fixture -> testCases.add(Arguments.of(fixture)));
            });
    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("unsupportedColumnsFixtures")
  public void unsupportedColumns(JsonContainerFixture fixture) {

    logFixture("unsupportedColumns", fixture);

    assertThat(fixture.unsupportedAllColumns())
        .as("Fixture has unsupported columns, fixture=%s", fixture)
        .isNotEmpty();

    var e =
        assertThrowsExactly(
            DocumentException.class,
            () -> buildRow(fixture),
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
            BaseFixtureIdentifiers.ALL_CLASSES, // all the different types of identifiers
            List.of(new UnsupportedTypesData()), // need unsupported types
            List.of(new AllUnsupportedTypes()) // need unsupported types;
            );

    cqlFixtures.forEach(
        cqlFixture -> {
          new AllColumns(cqlFixture).get().forEach(fixture -> testCases.add(Arguments.of(fixture)));
        });
    return testCases.stream();
  }

  @ParameterizedTest
  @MethodSource("outOfRangeColumnsFixtures")
  public void outOfRangeColumns(JsonContainerFixture fixture, boolean isOverflow) {

    logFixture("overflowColumns{isOverflow=" + isOverflow + "}", fixture);

    assertThat(fixture.outOfRangeAllColumns())
        .as("Fixture has one outOfRange columns, fixture=%s", fixture)
        .hasSize(1);

    // if the columns out of range are float or double then they will be positive infinity
    var outOfRangeMetadata = fixture.outOfRangeAllColumns().getFirst();
    boolean isInfinityType =
        CqlTypesForTesting.INFINITY_TYPES.contains(outOfRangeMetadata.getType());
    LOGGER.info(
        "overflowColumns: outOfRangeMetadata={} isInfinityType={}",
        outOfRangeMetadata,
        isInfinityType);

    if (isInfinityType) {
      var writableRow =
          assertDoesNotThrow(
              () -> buildRow(fixture),
              String.format(
                  "Throw not exception when row has outOfRange infinite type \nfixture=%s \ncontainer=%s \ntable=%s",
                  fixture.toString(true),
                  PrettyPrintable.pprint(fixture.container()),
                  fixture.cqlFixture().tableMetadata().describe(true)));

      var infinityValue =
          switch (writableRow.allColumns().get(outOfRangeMetadata.getName()).value()) {
            case Float f -> f;
            case Double d -> d;
            default ->
                throw new IllegalStateException(
                    "Unexpected value: "
                        + writableRow.allColumns().get(outOfRangeMetadata.getName()).value());
          };

      if (isOverflow) {
        assertPositiveInfinity(infinityValue);
      } else {
        assertNegativeInfinity(infinityValue);
      }

    } else {
      var e =
          assertThrowsExactly(
              DocumentException.class,
              () -> {
                buildRow(fixture);
              },
              String.format(
                  "Throw exception when row has outOfRange \nfixture=%s \ncontainer=%s \ntable=%s",
                  fixture.toString(true),
                  PrettyPrintable.pprint(fixture.container()),
                  fixture.cqlFixture().tableMetadata().describe(true)));
      assertThat(e.code)
          .as("Using correct error code")
          .isEqualTo(DocumentException.Code.INVALID_COLUMN_VALUES.name());

      fixture
          .outOfRangeAllColumns()
          .forEach(
              outOfRangeColumn -> {
                assertThat(e)
                    .as("outOfRange column included in message %s", outOfRangeColumn)
                    .hasMessageContaining(errFmt(outOfRangeColumn));
              });
    }
  }

  private void assertPositiveInfinity(Float value) {
    assertThat(value).as("Value is positive infinity for Float").isInfinite().isPositive();
  }

  private void assertPositiveInfinity(Double value) {
    assertThat(value).as("Value is positive infinity for Double").isInfinite().isPositive();
  }

  private void assertNegativeInfinity(Float value) {
    assertThat(value).as("Value is negative infinity for Float").isInfinite().isNegative();
  }

  private void assertNegativeInfinity(Double value) {
    assertThat(value).as("Value is negative infinity for Double").isInfinite().isNegative();
  }

  private static Stream<Arguments> outOfRangeColumnsFixtures() {
    List<Arguments> testCases = new ArrayList<>();

    var cqlOverflowFixtures =
        CqlFixture.allFixtures(
            BaseFixtureIdentifiers.ALL_CLASSES, // all the different types of identifiers
            List.of(new OverflowData()), // need unsupported types
            List.of(new AllOverflowTypes()) // need unsupported types;
            );

    cqlOverflowFixtures.forEach(
        cqlFixture -> {
          new EachNonKeyOutOfRange(cqlFixture)
              .get()
              .forEach(fixture -> testCases.add(Arguments.of(fixture, true)));
        });

    var cqlUnderflowFixtures =
        CqlFixture.allFixtures(
            BaseFixtureIdentifiers.ALL_CLASSES, // all the different types of identifiers
            List.of(new UnderflowData()), // need unsupported types
            List.of(new AllUnderflowTypes()) // need unsupported types;
            );

    cqlUnderflowFixtures.forEach(
        cqlFixture -> {
          new EachNonKeyOutOfRange(cqlFixture)
              .get()
              .forEach(fixture -> testCases.add(Arguments.of(fixture, false)));
        });

    return testCases.stream();
  }
}
