package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.OpenSearchTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDefContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiOpenSearchIndex;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests that {@code $openSearch} on a table resolves to an {@link OpenSearchTableFilter}. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class TableFilterResolverOpenSearchTest {

  private static final TestData TEST_DATA = new TestData();
  // table with no OpenSearch index
  private static final TableSchemaObject PLAIN_TABLE_SCHEMA_OBJECT =
      TEST_DATA.schemaObject().tableWithMapSetList();
  // same table, but reporting an OpenSearch index named "my_os_idx"
  private static final TableSchemaObject TABLE_SCHEMA_OBJECT =
      withOpenSearchIndex(PLAIN_TABLE_SCHEMA_OBJECT, "my_os_idx");
  private static final CommandContext<TableSchemaObject> CONTEXT =
      TEST_DATA.commandContext().tableSchemaObjectCommandContext(TABLE_SCHEMA_OBJECT);

  private static TableSchemaObject withOpenSearchIndex(
      TableSchemaObject table, String saiIndexName) {
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    when(indexMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(saiIndexName));
    when(indexMetadata.getOptions()).thenReturn(Map.of());

    var indexes = new ApiIndexDefContainer();
    indexes.put(ApiOpenSearchIndex.fromCqlMetadata(indexMetadata));

    ApiTableDef tableDef = spy(table.apiTableDef());
    doReturn(indexes).when(tableDef).indexes();
    TableSchemaObject spied = spy(table);
    doReturn(tableDef).when(spied).apiTableDef();
    return spied;
  }

  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;

  private OpenSearchTableFilter resolveSearchFilter(String filterJson) throws Exception {
    return resolveSearchFilter(CONTEXT, filterJson);
  }

  private OpenSearchTableFilter resolveSearchFilter(
      CommandContext<TableSchemaObject> context, String filterJson) throws Exception {
    FindCommand command =
        objectMapper.readValue(
            "{\"find\": {\"filter\": %s}}".formatted(filterJson), FindCommand.class);
    var resolver = new TableFilterResolver<FindCommand>(operationsConfig);
    DBLogicalExpression expression = resolver.resolve(context, command).target();

    List<OpenSearchTableFilter> found = new ArrayList<>();
    expression.visitAllFilters(OpenSearchTableFilter.class, found::add);
    assertThat(found).hasSize(1);
    return found.get(0);
  }

  @Test
  public void searchSubDocumentStaysJsonObject() throws Exception {
    OpenSearchTableFilter filter =
        resolveSearchFilter("{\"$openSearch\": {\"match\": {\"title\": \"Cassandra\"}}}");

    JsonNode expected = objectMapper.readTree("{\"match\": {\"title\": \"Cassandra\"}}");
    assertThat(filter.queryDsl().isObject()).isTrue();
    assertThat(filter.queryDsl()).isEqualTo(expected);
  }

  @Test
  public void searchMatchAllStaysJsonObject() throws Exception {
    OpenSearchTableFilter filter = resolveSearchFilter("{\"$openSearch\": {\"match_all\": {}}}");

    assertThat(filter.queryDsl()).isEqualTo(objectMapper.readTree("{\"match_all\": {}}"));
  }

  @Test
  public void searchNestedArraysAndNumbersPreserved() throws Exception {
    String dsl =
        "{\"bool\": {\"must\": [{\"match\": {\"title\": \"a\"}}], \"filter\": [{\"range\": {\"n\": {\"gt\": 3}}}]}}";
    OpenSearchTableFilter filter = resolveSearchFilter("{\"$openSearch\": %s}".formatted(dsl));

    // Compare serialized form: the filter builder yields BigDecimal numbers (DecimalNode), which
    // are not equal() to the IntNode that readTree() produces, but serialize identically.
    assertThat(filter.queryDsl().toString()).isEqualTo(objectMapper.readTree(dsl).toString());
  }

  @Test
  public void searchGetsSaiIndexNameFromTableSchema() throws Exception {
    OpenSearchTableFilter filter = resolveSearchFilter("{\"$openSearch\": {\"match_all\": {}}}");

    assertThat(filter.saiIndexName()).isEqualTo("my_os_idx");
  }

  @Test
  public void searchOnTableWithoutOpenSearchIndexFails() {
    var plainContext =
        TEST_DATA.commandContext().tableSchemaObjectCommandContext(PLAIN_TABLE_SCHEMA_OBJECT);

    Throwable t =
        catchThrowable(
            () -> resolveSearchFilter(plainContext, "{\"$openSearch\": {\"match_all\": {}}}"));

    assertThat(t)
        .isInstanceOfSatisfying(
            SchemaException.class,
            e ->
                assertThat(e.code)
                    .isEqualTo(SchemaException.Code.OPEN_SEARCH_NOT_ENABLED_FOR_TABLE.name()));
  }

  @Test
  public void searchEmitsExprPredicateWithJsonObjectLiteral() throws Exception {
    OpenSearchTableFilter filter =
        resolveSearchFilter("{\"$openSearch\": {\"match\": {\"title\": \"Cassandra\"}}}");

    StringBuilder cql = new StringBuilder();
    filter.apply(TABLE_SCHEMA_OBJECT, new ArrayList<>()).appendTo(cql);

    assertThat(cql.toString())
        .isEqualTo("expr(my_os_idx, '{\"match\":{\"title\":\"Cassandra\"}}')");
  }

  @Test
  public void searchEscapesSingleQuoteByDoubling() throws Exception {
    OpenSearchTableFilter filter =
        resolveSearchFilter("{\"$openSearch\": {\"match\": {\"title\": \"it's\"}}}");

    StringBuilder cql = new StringBuilder();
    filter.apply(TABLE_SCHEMA_OBJECT, new ArrayList<>()).appendTo(cql);

    assertThat(cql.toString()).isEqualTo("expr(my_os_idx, '{\"match\":{\"title\":\"it''s\"}}')");
  }
}
