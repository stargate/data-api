package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatCommandResult;
import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatSchemaException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.CreateCollectionCommandResolver;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionIndexingConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NOTE: Example table comment string:
 *
 * <pre>
 *  {
 * 	"collection": {
 * 		"name": "collection-test-id-KLX4CjpEiAudPWwp",
 * 		"schema_version": "2",
 * 		"options": {
 * 			"defaultId": {
 * 				"type": ""
 * 			            },
 * 			"lexical": {
 * 				"enabled": true,
 * 				"analyzer": "standard"
 *            },
 * 			"rerank": {
 * 				"enabled": true,
 * 				"service": {
 * 					"provider": "nvidia",
 * 					"modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
 * 					"authentication": null,
 * 					"parameters": null
 *                }
 *            }
 * 	    }
 * }}
 * </pre>
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CreateCollectionOperationTest extends OperationTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateCollectionOperationTest.class);

  // Need the CreateCollectionCommandResolver so we can use it to set defaults on values
  @Inject CreateCollectionCommandResolver createCollectionCommandResolver;

  @Inject DatabaseLimitsConfig databaseLimitsConfig;

  @Inject ObjectMapper objectMapper;

  // Comment to extract comment from the crete table cql statement.
  // Assume it is delineated by single quotes
  private static final Pattern TABLE_COMMENT_PATTERN = Pattern.compile("comment='(.*?)'");

  private SchemaChangeMemento assertOperation(
      String testName,
      CreateCollectionOperation operation,
      int expectedCreateTable,
      int expectedCreateIndex) {
    return assertOperation(
        testName, operation, expectedCreateTable, expectedCreateIndex, true, false);
  }

  private SchemaChangeMemento assertOperation(
      String testName,
      CreateCollectionOperation operation,
      int expectedCreateTable,
      int expectedCreateIndex,
      boolean mockKeyspaceMetadata,
      boolean awaitFailure) {
    LOGGER.info(
        "assertOperation() - testName={}, expectedCreateTable={}, expectedCreateIndex={}",
        testName,
        expectedCreateTable,
        expectedCreateIndex);

    // track calls to change the schema and mock the keyspace exists for the new collection
    var mockQueryExecutor = mock(QueryExecutor.class);
    var schemaChangeMemento = addSchemaChangeMomento(mockQueryExecutor);
    addMockKeyspaceMetadata(mockQueryExecutor, mockKeyspaceMetadata);

    // IMPORTANT - we still need to confirm the DB calls even in an error condition
    // so catch, check db activity, then rethrow
    var subscriber =
        awaitFailure
            ? operation
                .execute(requestContext, mockQueryExecutor)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitFailure()
            : operation
                .execute(requestContext, mockQueryExecutor)
                .subscribe()
                .withSubscriber(UniAssertSubscriber.create())
                .awaitItem();

    LOGGER.info(
        "assertOperation() - testName={}, schemaChangeMemento={}", testName, schemaChangeMemento);

    // Validate the change schema calls
    assertThat(schemaChangeMemento.counter.get())
        .as("%s - total schema calls is expected")
        .isEqualTo(expectedCreateTable + expectedCreateIndex);

    int actualCreateTables =
        schemaChangeMemento.queries.stream()
            .filter(query -> query.startsWith("CREATE TABLE"))
            .toList()
            .size();
    assertThat(actualCreateTables)
        .as("%s - expected create table calls", testName)
        .isEqualTo(expectedCreateTable);

    // create table is always first
    if (expectedCreateTable > 0) {
      var collectionComment = schemaChangeMemento.tableComments.getFirst();
      assertThat(collectionComment)
          .as("%s - collection comment is not blank", testName)
          .isNotBlank()
          .as("Collection comment is not blank");
    }

    int actualCreateIndexes =
        schemaChangeMemento.queries.stream()
            .filter(query -> query.startsWith("CREATE CUSTOM INDEX"))
            .toList()
            .size();
    assertThat(actualCreateIndexes)
        .as("%s - expected create index calls", testName)
        .isEqualTo(expectedCreateIndex);

    // if the call failed, then we re-throw
    if (awaitFailure) {
      var throwable = subscriber.getFailure();
      LOGGER.info("assertOperation() - testName={}, throwable={}", testName, throwable.toString());

      assertThat(throwable)
          .as("%s - expected failure is APIException", testName)
          .isInstanceOf(APIException.class);
      throw (APIException) throwable;
    }

    // no failure, so check the command result is success
    var commandResult = subscriber.getItem().get();
    LOGGER.info("assertOperation() - testName={}, commandResult={}", testName, commandResult);
    assertThatCommandResult(commandResult)
        .as(testName)
        .isDDLSuccess()
        .hasOnlyStatus(CommandStatus.OK, 1);

    return schemaChangeMemento;
  }

  @Test
  public void successWithLexicalNoVector() {

    var operation =
        new CreateCollectionOperation(
            KEYSPACE_CONTEXT,
            databaseLimitsConfig,
            TEST_CONSTANTS.COLLECTION_IDENTIFIER.table(),
            10,
            false,
            null,
            null,
            null,
            CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null),
            CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null));

    // 1 create Table + 8 super shredder indexes + lexical index
    var schemaChangeMemento = assertOperation("successWithLexicalNoVector", operation, 1, 9);

    var commentNode =
        collectionNodeFromTableComment("successWithLexicalNoVector", schemaChangeMemento);
    var optionsNode = commentNode.get(TableCommentConstants.OPTIONS_KEY);
    assertThat(optionsNode.get(TableCommentConstants.COLLECTION_VECTOR_KEY))
        .as("Collection comment must not have a vector key")
        .isNull();
  }

  @Test
  public void successWithLexicalWithVector() {

    var vectorDesc = new CreateCollectionCommand.Options.VectorSearchDesc(5, "cosine", null, null);
    // Must use validateVectorOptions() because it will cleanup defaults, the resolver normally does
    // this.
    vectorDesc = createCollectionCommandResolver.validateVectorOptions(vectorDesc);

    var operation =
        new CreateCollectionOperation(
            KEYSPACE_CONTEXT,
            databaseLimitsConfig,
            TEST_CONSTANTS.COLLECTION_IDENTIFIER.table(),
            10,
            false,
            null,
            null,
            vectorDesc,
            CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null),
            CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null));

    // 1 create Table + 8 super shredder indexes + 1 vector index + 1 lexical
    var schemaChangeMemento = assertOperation("successWithLexicalWithVector", operation, 1, 10);

    var commentNode =
        collectionNodeFromTableComment("successWithLexicalWithVector", schemaChangeMemento);
    var optionsNode = commentNode.get(TableCommentConstants.OPTIONS_KEY);

    var vectorNode = optionsNode.get(TableCommentConstants.COLLECTION_VECTOR_KEY);
    assertThat(vectorNode).as("Collection comment must have a vector key").isNotNull();

    // see CollectionSettingsV1Reader
    var vectorColumnDefinition = VectorColumnDefinition.fromJson(vectorNode, objectMapper);

    assertThat(vectorColumnDefinition.vectorSize())
        .as("Vector size from table comment matches")
        .isEqualTo(5);
    assertThat(vectorColumnDefinition.similarityFunction())
        .as("Similarity function from table comment matches")
        .isEqualTo(SimilarityFunction.COSINE);
    assertThat(vectorColumnDefinition.sourceModel())
        .as("Source model from table comment is DEFAULT (currently OTHER)")
        .isEqualTo(EmbeddingSourceModel.DEFAULT);
    assertThat(vectorColumnDefinition.vectorizeDefinition())
        .as("Vectorize definition from table comment matches")
        .isNull();
  }

  @Test
  public void successIndexingDenyAllWithLexical() {

    var indexingDesc = new CreateCollectionCommand.Options.IndexingDesc(null, List.of("*"));
    var operation =
        new CreateCollectionOperation(
            KEYSPACE_CONTEXT,
            databaseLimitsConfig,
            TEST_CONSTANTS.COLLECTION_IDENTIFIER.table(),
            10,
            false,
            null,
            indexingDesc,
            null,
            CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null),
            CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null));

    // 1 create Table + 1 lexical index
    // NOTE: because of deny all we do not need any super shredding, but we do need the lexcial
    // still
    // for the $lexcial field
    var schemaChangeMemento = assertOperation("successIndexingDenyAllWithLexical", operation, 1, 1);

    // see CollectionSettingsV1Reader
    var commentNode =
        collectionNodeFromTableComment("successIndexingDenyAllWithLexical", schemaChangeMemento);
    var optionsNode = commentNode.get(TableCommentConstants.OPTIONS_KEY);

    var indexingNode = optionsNode.get(TableCommentConstants.COLLECTION_INDEXING_KEY);
    assertThat(indexingNode).as("Collection comment must not have a indexing key").isNotNull();

    var indexingConfig = CollectionIndexingConfig.fromJson(indexingNode);
    assertThat(indexingConfig.allowed())
        .as("Collection indexing allow must match table comment")
        .isEqualTo(Set.of());

    assertThat(indexingConfig.denied())
        .as("Collection indexing deny must match table comment")
        .isEqualTo(Set.of("*"));
  }

  @Test
  public void successIndexingDenyAllWithLexicalWithVector() {

    var vectorDesc = new CreateCollectionCommand.Options.VectorSearchDesc(5, "cosine", null, null);
    // Must use validateVectorOptions() because it will cleanup defaults, the resolver normally does
    // this.
    vectorDesc = createCollectionCommandResolver.validateVectorOptions(vectorDesc);

    var indexingDesc = new CreateCollectionCommand.Options.IndexingDesc(null, List.of("*"));
    var operation =
        new CreateCollectionOperation(
            KEYSPACE_CONTEXT,
            databaseLimitsConfig,
            TEST_CONSTANTS.COLLECTION_IDENTIFIER.table(),
            10,
            false,
            null,
            indexingDesc,
            vectorDesc,
            CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null),
            CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null));

    // 1 create Table + 1 vector index + 1 lexical
    // NOTE: because of deny all we do not need any super shredding, but we do need the lexcial
    // still
    // for the $lexcial field
    var schemaChangeMemento =
        assertOperation("successIndexingDenyAllWithLexicalWithVector", operation, 1, 2);

    // NOTE: no need to test the table comment again, that is covered above
  }

  @Test
  public void failMissingKeyspace() {

    var operation =
        new CreateCollectionOperation(
            KEYSPACE_CONTEXT,
            databaseLimitsConfig,
            TEST_CONSTANTS.COLLECTION_IDENTIFIER.table(),
            10,
            false,
            null,
            null,
            null,
            CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null),
            CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null));

    // this should throw, not return Command
    var exception =
        catchThrowable(() -> assertOperation("failMissingKeyspace", operation, 0, 0, false, true));

    assertThatSchemaException(exception)
        .as("failMissingKeyspace()")
        .hasCode(SchemaException.Code.UNKNOWN_KEYSPACE)
        .hasMessageSnippets(
            "The keyspace used by the command: %s."
                .formatted(KEYSPACE_CONTEXT.schemaObject().identifier().keyspace()));
  }

  /**
   * Test: create table works, but there is an index with the same name, the operation should then
   * try to drop the table. More complicated than others.
   */
  @Test
  public void failExistingIndexDropTable() {

    var queryExecutor = mock(QueryExecutor.class);
    var successResultSet = mockSchemaSuccessResultSet();
    addMockKeyspaceMetadata(queryExecutor);

    var schemaChangeCounter = new AtomicInteger();
    var schemaDropCounter = new AtomicInteger();

    // count the first create table
    when(queryExecutor.executeCreateSchemaChange(
            eq(requestContext),
            argThat(
                simpleStatement ->
                    simpleStatement.getQuery().startsWith("CREATE TABLE IF NOT EXISTS"))))
        .then(
            invocation -> {
              schemaChangeCounter.incrementAndGet();
              return Uni.createFrom().item(successResultSet);
            });

    // mock existing index from first create index
    when(queryExecutor.executeCreateSchemaChange(
            eq(requestContext),
            argThat(
                simpleStatement -> simpleStatement.getQuery().startsWith("CREATE CUSTOM INDEX"))))
        .then(
            invocation -> {
              schemaChangeCounter.incrementAndGet();
              throw new InvalidQueryException(mock(Node.class), "Index xxxxx already exists");
            });

    // count the drop table
    when(queryExecutor.executeDropSchemaChange(
            eq(requestContext),
            argThat(
                simpleStatement -> simpleStatement.getQuery().startsWith("DROP TABLE IF EXISTS"))))
        .then(
            invocation -> {
              schemaDropCounter.incrementAndGet();
              return Uni.createFrom().item(successResultSet);
            });

    var operation =
        new CreateCollectionOperation(
            KEYSPACE_CONTEXT,
            databaseLimitsConfig,
            TEST_CONSTANTS.COLLECTION_IDENTIFIER.table(),
            10,
            true,
            null,
            null,
            null,
            CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null),
            CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null));

    operation
        .execute(requestContext, queryExecutor)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();

    // 1 create Table + 1 index failure
    assertThat(schemaChangeCounter.get()).isEqualTo(2);
    // 1 drop table
    assertThat(schemaDropCounter.get()).isEqualTo(1);
  }

  private void addMockKeyspaceMetadata(QueryExecutor queryExecutor) {
    addMockKeyspaceMetadata(queryExecutor, true);
  }

  /** Attaches mock KeyspaceMetadata to the queryExecutor so create tests can find the keyspace */
  private void addMockKeyspaceMetadata(QueryExecutor queryExecutor, boolean addKeyspaceMetadata) {

    var allKeyspaces = new HashMap<CqlIdentifier, KeyspaceMetadata>();
    if (addKeyspaceMetadata) {
      var keyspaceMetadata =
          new DefaultKeyspaceMetadata(
              TEST_CONSTANTS.KEYSPACE_IDENTIFIER.keyspace(),
              false,
              false,
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>());
      allKeyspaces.put(keyspaceMetadata.getName(), keyspaceMetadata);
    }

    var driverMetadata = mock(Metadata.class);
    when(driverMetadata.getKeyspaces()).thenReturn(allKeyspaces);

    when(queryExecutor.getDriverMetadata(any())).thenReturn(Uni.createFrom().item(driverMetadata));
  }

  private AsyncResultSet mockSchemaSuccessResultSet() {

    var schemaColumns = buildColumnDefs(OperationTestBase.TestColumn.ofBoolean("[applied]"));
    List<Row> resultRows = List.of(new MockRow(schemaColumns, 0, List.of(byteBufferFrom(true))));
    return new MockAsyncResultSet(schemaColumns, resultRows, null);
  }

  private record SchemaChangeMemento(
      AtomicInteger counter, List<String> queries, List<String> tableComments) {
    SchemaChangeMemento {
      counter = counter == null ? new AtomicInteger() : counter;
      queries = queries == null ? new ArrayList<>() : queries;
      tableComments = tableComments == null ? new ArrayList<>() : tableComments;
    }
  }

  private SchemaChangeMemento addSchemaChangeMomento(QueryExecutor queryExecutor) {
    var memento = new SchemaChangeMemento(null, null, null);

    when(queryExecutor.executeCreateSchemaChange(eq(requestContext), any()))
        .then(
            invocation -> {
              memento.counter.incrementAndGet();
              SimpleStatement statement = invocation.getArgument(1);
              memento.queries.add(statement.getQuery());
              var matcher = TABLE_COMMENT_PATTERN.matcher(statement.getQuery());
              memento.tableComments.add(matcher.find() ? matcher.group(1) : null);
              return Uni.createFrom().item(mockSchemaSuccessResultSet());
            });
    return memento;
  }

  private JsonNode collectionNodeFromTableComment(String testName, SchemaChangeMemento memento) {
    // create table should be first
    return collectionNodeFromTableComment(testName, memento.tableComments.getFirst());
  }

  private JsonNode collectionNodeFromTableComment(String testName, String tableComment) {

    LOGGER.info("tableCommentToNode() - testName: {}, tableComment: {}", testName, tableComment);
    try {
      var root = objectMapper.readTree(tableComment);
      // we always want the "collection" node, see example at the top
      // let the null out, it will cause the calling test to fail loud
      return root.get(TableCommentConstants.TOP_LEVEL_KEY);
    } catch (JacksonException e) {
      throw new RuntimeException(
          "Invalid JSON in Table comment for Collection, problem: " + e.getMessage());
    }
  }
}
