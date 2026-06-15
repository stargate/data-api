package io.stargate.sgv2.jsonapi.service.operation.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
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
import org.junit.jupiter.api.BeforeEach;
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
  private static final Pattern COMMENT_PATTERN = Pattern.compile("comment='(.*?)'");

  private final ColumnDefinitions RESULT_COLUMNS =
      buildColumnDefs(OperationTestBase.TestColumn.ofBoolean("[applied]"));

  private AsyncResultSet mockSuccessSchemaResultset() {
    List<Row> resultRows =
        Arrays.asList(new MockRow(RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

    return new MockAsyncResultSet(RESULT_COLUMNS, resultRows, null);
  }

  private record SchemaChangeMemento(AtomicInteger counter, List<String> cqlComments) {
    SchemaChangeMemento {
      counter = counter == null ? new AtomicInteger() : counter;
      cqlComments = cqlComments == null ? new ArrayList<>() : cqlComments;
    }
  }

  private SchemaChangeMemento addSchemaChangeMomento(QueryExecutor queryExecutor) {
    var memento = new SchemaChangeMemento(null, null);

    when(queryExecutor.executeCreateSchemaChange(eq(requestContext), any()))
        .then(
            invocation -> {
              memento.counter.incrementAndGet();
              SimpleStatement statement = invocation.getArgument(1);
              var matcher = COMMENT_PATTERN.matcher(statement.getQuery());
              memento.cqlComments.add(matcher.find() ? matcher.group(1) : null);
              return Uni.createFrom().item(mockSuccessSchemaResultset());
            });
    return memento;
  }

  private void addKeyspaceSchema(QueryExecutor queryExecutor) {

    var driverMetadata = mock(Metadata.class);
    when(queryExecutor.getDriverMetadata(any())).thenReturn(Uni.createFrom().item(driverMetadata));

    var allKeyspaces = new HashMap<CqlIdentifier, KeyspaceMetadata>();
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
    when(driverMetadata.getKeyspaces()).thenReturn(allKeyspaces);
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

  @BeforeEach
  public void init() {}

  @Test
  public void createCollectionNoVector() {

    var queryExecutor = mock(QueryExecutor.class);
    var schemaChangeMemento = addSchemaChangeMomento(queryExecutor);
    addKeyspaceSchema(queryExecutor);

    // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
    // :(
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

    operation
        .execute(requestContext, queryExecutor)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();

    // 1 create Table + 8 super shredder indexes + lexical index
    assertThat(schemaChangeMemento.counter.get()).isEqualTo(10);

    var collectionComment = schemaChangeMemento.cqlComments.getFirst();
    assertThat(collectionComment).isNotBlank().as("Collection comment is not blank");

    var commentNode = collectionNodeFromTableComment("createCollectionNoVector", collectionComment);
    var optionsNode = commentNode.get(TableCommentConstants.OPTIONS_KEY);
    assertThat(optionsNode.get(TableCommentConstants.COLLECTION_VECTOR_KEY))
        .as("Collection comment must not have a vector key")
        .isNull();
  }

  @Test
  public void createCollectionVector() {

    var queryExecutor = mock(QueryExecutor.class);
    var schemaChangeMemento = addSchemaChangeMomento(queryExecutor);
    addKeyspaceSchema(queryExecutor);

    // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
    // :(

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

    operation
        .execute(requestContext, queryExecutor)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();

    // 1 create Table + 8 super shredder indexes + 1 vector index + 1 lexical
    assertThat(schemaChangeMemento.counter.get()).isEqualTo(11);

    var collectionComment = schemaChangeMemento.cqlComments.getFirst();
    assertThat(collectionComment).isNotBlank().as("Collection comment is not blank");

    var commentNode = collectionNodeFromTableComment("createCollectionVector", collectionComment);
    var optionsNode = commentNode.get(TableCommentConstants.OPTIONS_KEY);
    var vectorNode = optionsNode.get(TableCommentConstants.COLLECTION_VECTOR_KEY);
    assertThat(vectorNode).as("Collection comment must have a vector key").isNotNull();

    // see CollectionSettingsV1Reader
    var vectorColumnDefinition = VectorColumnDefinition.fromJson(vectorNode, objectMapper);
    var vectorConfig = VectorConfig.fromColumnDefinitions(List.of(vectorColumnDefinition));

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
  public void denyAllCollectionNoVector() {
    var queryExecutor = mock(QueryExecutor.class);
    var schemaChangeMemento = addSchemaChangeMomento(queryExecutor);
    addKeyspaceSchema(queryExecutor);

    // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
    // :(

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

    operation
        .execute(requestContext, queryExecutor)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();

    // 1 create Table + 1 lexical index
    assertThat(schemaChangeMemento.counter.get()).isEqualTo(2);

    var collectionComment = schemaChangeMemento.cqlComments.getFirst();
    assertThat(collectionComment).isNotBlank().as("Collection comment is not blank");

    // see CollectionSettingsV1Reader
    var commentNode =
        collectionNodeFromTableComment("denyAllCollectionNoVector", collectionComment);
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
  public void denyAllCollectionVector() {

    var queryExecutor = mock(QueryExecutor.class);
    var schemaChangeMemento = addSchemaChangeMomento(queryExecutor);
    addKeyspaceSchema(queryExecutor);

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

    operation
        .execute(requestContext, queryExecutor)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();

    // 1 create Table + 1 vector index + 1 lexical
    assertThat(schemaChangeMemento.counter.get()).isEqualTo(3);

    // NOTE: no need to test the table comment again, that is covered above
  }

  @Test
  public void indexAlreadyDropTable() {

    var queryExecutor = mock(QueryExecutor.class);
    var successResultSet = mockSuccessSchemaResultset();
    addKeyspaceSchema(queryExecutor);

    final AtomicInteger schemaChangeCounter = new AtomicInteger();
    final AtomicInteger schemaDropCounter = new AtomicInteger();

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

    when(queryExecutor.executeCreateSchemaChange(
            eq(requestContext),
            argThat(
                simpleStatement -> simpleStatement.getQuery().startsWith("CREATE CUSTOM INDEX"))))
        .then(
            invocation -> {
              schemaChangeCounter.incrementAndGet();
              throw new InvalidQueryException(mock(Node.class), "Index xxxxx already exists");
            });

    when(queryExecutor.executeDropSchemaChange(
            eq(requestContext),
            argThat(
                simpleStatement -> simpleStatement.getQuery().startsWith("DROP TABLE IF EXISTS"))))
        .then(
            invocation -> {
              schemaDropCounter.incrementAndGet();
              return Uni.createFrom().item(successResultSet);
            });

    // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
    // :(
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

  private List<ColumnMetadata> createCorrectPartitionColumn() {
    List<DataType> tuple =
        Arrays.asList(
            new PrimitiveType(ProtocolConstants.DataType.TINYINT),
            new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
    List<ColumnMetadata> partitionKey = new ArrayList<>();
    partitionKey.add(
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal("keyspace"),
            CqlIdentifier.fromInternal("collection"),
            CqlIdentifier.fromInternal("key"),
            new DefaultTupleType(tuple),
            false));
    return partitionKey;
  }
}
