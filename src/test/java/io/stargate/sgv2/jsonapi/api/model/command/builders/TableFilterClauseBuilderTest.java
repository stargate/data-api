package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class TableFilterClauseBuilderTest {

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class TableMapSetListFilter {

    // initialization of the test data is in individual methodResource
    private TableSchemaObject mapSetListTableSchema;
    private String listColumnName;
    private String setColumnName;
    private String mapColumnName;

    private Stream<Object> invalidJsonMissingArray() {
      TestData td = new TestData();
      mapSetListTableSchema = td.schemaObject().tableWithMapSetList();
      listColumnName = td.names.CQL_LIST_COLUMN.asInternal();
      setColumnName = td.names.CQL_SET_COLUMN.asInternal();
      mapColumnName = td.names.CQL_MAP_COLUMN.asInternal();
      return Stream.of(
          // list
          Arguments.of(
              "{ \"%s\": { \"$all\": \"apple\" } }".formatted(listColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$in\": {\"key\" : \"value\"}}}".formatted(listColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$in\": 123 } }".formatted(listColumnName), mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$nin\": \"cherry\" } }".formatted(listColumnName),
              mapSetListTableSchema),
          // set
          Arguments.of(
              "{ \"%s\": { \"$all\": \"apple\" } }".formatted(setColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$in\": {\"key\" : \"value\"}}}".formatted(setColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$in\": 123 } }".formatted(setColumnName), mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$nin\": \"cherry\" } }".formatted(setColumnName),
              mapSetListTableSchema),
          // map
          Arguments.of(
              "{ \"%s\": { \"$keys\": {\"$in\" : \"cherry\"}}}".formatted(mapColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$keys\": {\"$nin\" : \"cherry\"}}}".formatted(mapColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$keys\": {\"$all\" : \"cherry\"}}}".formatted(mapColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{ \"%s\": { \"$values\": {\"$in\" : \"cherry\"} } }".formatted(mapColumnName),
              mapSetListTableSchema));
    }

    @ParameterizedTest
    @MethodSource("invalidJsonMissingArray")
    public void ArrayValueRequiredForAllMapSetListOperators(
        String filterJson, TableSchemaObject tableSchemaObject) throws Exception {
      Throwable throwable =
          catchThrowable(() -> buildTableFilterClause(filterJson, tableSchemaObject));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              e ->
                  assertThat(((FilterException) e).code)
                      .isEqualTo(FilterException.Code.INVALID_MAP_SET_LIST_FILTER.name()))
          .satisfies(
              e -> assertThat((e).getMessage()).contains("be followed by an array of value"));
    }

    private Stream<Arguments> invalidOperators() {
      TestData td = new TestData();
      mapSetListTableSchema = td.schemaObject().tableWithMapSetList();
      listColumnName = td.names.CQL_LIST_COLUMN.asInternal();
      setColumnName = td.names.CQL_SET_COLUMN.asInternal();
      mapColumnName = td.names.CQL_MAP_COLUMN.asInternal();
      // $in, $nin, $all are valid operators for list, set and map filters.
      List<String> invalidOperators =
          List.of(
              // Value comparison operators
              "$eq",
              "$ne",
              "$gt",
              "$gte",
              "$lt",
              "$lte",
              // Array comparison operators
              "$size",
              // Element comparison operators
              "$exists",
              // some random operators
              "$abc",
              "$any",
              "$keys",
              "$values",
              "$entries",
              "$contains",
              "$containsKey");
      List<String> invalidOperatorFilterForList =
          invalidOperators.stream()
              .map(
                  operator ->
                      "{ \"%s\": { \"%s\": [\"apple\",\"cherry\"] } }"
                          .formatted(listColumnName, operator))
              .toList();
      List<String> invalidOperatorFilterForSet =
          invalidOperators.stream()
              .map(
                  operator ->
                      "{ \"%s\": { \"%s\": [\"apple\",\"cherry\"] } }"
                          .formatted(listColumnName, operator))
              .toList();
      List<String> invalidOperatorFilterForMapKeys =
          invalidOperators.stream()
              .map(
                  operator ->
                      "{ \"%s\": {\"$keys\": {\"%s\": [\"apple\",\"cherry\"] } } }"
                          .formatted(listColumnName, operator))
              .toList();
      List<String> invalidOperatorFilterForMapValues =
          invalidOperators.stream()
              .map(
                  operator ->
                      "{ \"%s\": {\"$values\": {\"%s\": [\"apple\",\"cherry\"] } } }"
                          .formatted(listColumnName, operator))
              .toList();

      List<String> invalidOperatorFilterForMapEntries =
          invalidOperators.stream()
              .map(
                  operator ->
                      "{ \"%s\": {\"%s\": [[\"key1\",\"value1\"], [\"key2\",\"value2\"]] } }"
                          .formatted(listColumnName, operator))
              .toList();

      // Combine all into one stream of Arguments
      return Stream.of(
              invalidOperatorFilterForList,
              invalidOperatorFilterForSet,
              invalidOperatorFilterForMapKeys,
              invalidOperatorFilterForMapValues,
              invalidOperatorFilterForMapEntries)
          .flatMap(
              list -> list.stream().map(filter -> Arguments.of(filter, mapSetListTableSchema)));
    }

    @ParameterizedTest
    @MethodSource("invalidOperators")
    public void invalidOperatorsForMapSetList(
        String filterJson, TableSchemaObject tableSchemaObject) throws Exception {
      Throwable throwable =
          catchThrowable(() -> buildTableFilterClause(filterJson, tableSchemaObject));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              e ->
                  assertThat(((FilterException) e).code)
                      .isEqualTo(FilterException.Code.INVALID_MAP_SET_LIST_FILTER.name()))
          .satisfies(
              e ->
                  assertThat((e).getMessage())
                      .contains("allowed operators are '$all', '$in', '$nin'"));
    }

    private Stream<Object> mapColumnTest() {
      TestData td = new TestData();
      mapSetListTableSchema = td.schemaObject().tableWithMapSetList();
      listColumnName = td.names.CQL_LIST_COLUMN.asInternal();
      setColumnName = td.names.CQL_SET_COLUMN.asInternal();
      mapColumnName = td.names.CQL_MAP_COLUMN.asInternal();

      return Stream.of(
          Arguments.of(
              "{\"%s\": { \"$keys\": 123}}".formatted(mapColumnName),
              "Invalid filter for map column",
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": { \"$keys\": []}}".formatted(mapColumnName),
              "Invalid filter for map column",
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": { \"$valueee\": {\"$in\" : \"cherry\"} } }".formatted(mapColumnName),
              "Invalid filter operator '$valueee'",
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": { \"$key\": {\"$in\" : \"cherry\"} } }".formatted(mapColumnName),
              "Invalid filter operator '$key'",
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": {\"$in\": [[\"key1\",\"value1\"], [\"key2\",\"extra\",\"value2\"]] }}"
                  .formatted(mapColumnName),
              "Invalid usage for map entry filter in tuple format",
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": {\"$in\": [[\"key1\",\"value1\"], []]}}".formatted(mapColumnName),
              "Invalid usage for map entry filter in tuple format",
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": {\"$in\": {\"key1\" : \"value1\"}}}".formatted(mapColumnName),
              "Invalid usage for map entry filter in tuple format",
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": {\"$in\": [[\"key1\",\"value1\"], 123]}}".formatted(mapColumnName),
              "Invalid usage for map entry filter in tuple format",
              mapSetListTableSchema));
    }

    @ParameterizedTest
    @MethodSource("mapColumnTest")
    public void mapColumnTest(
        String filterJson, String msgSnippet, TableSchemaObject tableSchemaObject)
        throws Exception {
      Throwable throwable =
          catchThrowable(() -> buildTableFilterClause(filterJson, tableSchemaObject));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              e ->
                  assertThat(((FilterException) e).code)
                      .isEqualTo(FilterException.Code.INVALID_MAP_SET_LIST_FILTER.name()))
          .satisfies(e -> assertThat((e).getMessage()).contains(msgSnippet));
    }

    private Stream<Object> listValueRequired() {
      TestData td = new TestData();
      mapSetListTableSchema = td.schemaObject().tableWithMapSetList();
      listColumnName = td.names.CQL_LIST_COLUMN.asInternal();
      setColumnName = td.names.CQL_SET_COLUMN.asInternal();
      mapColumnName = td.names.CQL_MAP_COLUMN.asInternal();

      return Stream.of(
          Arguments.of(
              "{\"%s\": {\"$values\": {\"$all\" : \"cherry\"}}}".formatted(mapColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": {\"$keys\": {\"$in\" : \"cherry\"}}}".formatted(mapColumnName),
              mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": {\"$all\" : {} }}}".formatted(listColumnName), mapSetListTableSchema),
          Arguments.of(
              "{\"%s\": {\"$nin\" : \"apple\" }}}".formatted(setColumnName),
              mapSetListTableSchema));
    }

    @ParameterizedTest
    @MethodSource("listValueRequired")
    public void mapSetListFilterOperatorFollowsAListValue(
        String filterJson, TableSchemaObject tableSchemaObject) throws Exception {
      Throwable throwable =
          catchThrowable(() -> buildTableFilterClause(filterJson, tableSchemaObject));
      assertThat(throwable)
          .isInstanceOf(FilterException.class)
          .satisfies(
              e ->
                  assertThat(((FilterException) e).code)
                      .isEqualTo(FilterException.Code.INVALID_MAP_SET_LIST_FILTER.name()))
          .satisfies(
              e -> assertThat((e).getMessage()).contains("must be followed by an array of values"));
    }

    @Test
    public void multipleMapSetListFilters() throws Exception {
      TestData td = new TestData();
      mapSetListTableSchema = td.schemaObject().tableWithMapSetList();
      listColumnName = td.names.CQL_LIST_COLUMN.asInternal();
      setColumnName = td.names.CQL_SET_COLUMN.asInternal();
      mapColumnName = td.names.CQL_MAP_COLUMN.asInternal();
      String filterJson =
              """
              {
               "%s" : {"$in" : ["apple", "orange"], "$nin" : ["banana", "watermelon"]},
                "%s" : {"$all" : ["banana", "watermelon"]},
                "%s" : {"$keys" : {"$in" : ["key1", "key2"], "$nin" : ["key3", "key4"]}}
              }
              """
              .formatted(listColumnName, setColumnName, mapColumnName);
      FilterClause filterClause = buildTableFilterClause(filterJson, mapSetListTableSchema);
      assertThat(filterClause).isNotNull();
      assertThat(filterClause.logicalExpression().logicalExpressions).hasSize(0);
      assertThat(filterClause.logicalExpression().comparisonExpressions).hasSize(5);

      FilterOperator[] expectedOperators = {
        ValueComparisonOperator.IN,
        ValueComparisonOperator.NIN,
        ArrayComparisonOperator.ALL,
        ValueComparisonOperator.IN,
        ValueComparisonOperator.NIN
      };
      MapSetListFilterComponent[] expectedComponent = {
        MapSetListFilterComponent.LIST_VALUE,
        MapSetListFilterComponent.LIST_VALUE,
        MapSetListFilterComponent.SET_VALUE,
        MapSetListFilterComponent.MAP_KEY,
        MapSetListFilterComponent.MAP_KEY
      };
      for (int i = 0; i < 5; i++) {
        var comparisonExpression = filterClause.logicalExpression().comparisonExpressions.get(i);
        assertThat(comparisonExpression.getFilterOperations().get(0).operator())
            .isEqualTo(expectedOperators[i]);
        assertThat(comparisonExpression.getFilterOperations().get(0).mapSetListComponent())
            .isEqualTo(expectedComponent[i]);
      }
    }
  }

  private FilterClause buildTableFilterClause(String json, TableSchemaObject tableSchemaObject)
      throws IOException {
    return FilterClauseBuilder.builderFor(tableSchemaObject)
        .build(operationsConfig, objectMapper.readTree(json));
  }
}
