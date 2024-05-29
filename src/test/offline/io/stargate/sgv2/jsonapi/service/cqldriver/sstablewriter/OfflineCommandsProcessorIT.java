package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.response.BeginOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.EndOfflineSessionResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineGetStatusResponse;
import io.stargate.sgv2.jsonapi.api.response.OfflineInsertManyResponse;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class OfflineCommandsProcessorIT {

  private static String SSTABLES_TEST_DIRECTORY;

  @BeforeAll
  public static void setup(@TempDir File tempDir) {
    SSTABLES_TEST_DIRECTORY = tempDir.getAbsolutePath() + File.separator + "sstables";
    File sstablesTestDirectory = new File(SSTABLES_TEST_DIRECTORY);
    if (!sstablesTestDirectory.exists()) {
      if (!sstablesTestDirectory.mkdirs()) {
        throw new RuntimeException("Failed to create directory: " + SSTABLES_TEST_DIRECTORY);
      }
    }
  }

  @AfterAll
  public static void cleanup() {
    File sstablesTestDirectory = new File(SSTABLES_TEST_DIRECTORY);
    deleteRecursively(sstablesTestDirectory);
  }

  private static void deleteRecursively(File sstablesTestDirectory) {
    if (sstablesTestDirectory.isDirectory()) {
      File[] files = sstablesTestDirectory.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteRecursively(file);
        }
      }
    }
    if (!sstablesTestDirectory.delete()) {
      throw new RuntimeException("Failed to delete directory: " + sstablesTestDirectory);
    }
  }

  public static Stream<Arguments> testScenarios() {
    return Stream.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  @ParameterizedTest
  @MethodSource("testScenarios")
  public void testOfflineCommandsProcessor(boolean isVectorTable, boolean includeVectorData)
      throws ExecutionException, InterruptedException, IOException {
    String testId = UUID.randomUUID().toString();
    String namespace = "test_namespace";
    String sstablesOutputDirectory = SSTABLES_TEST_DIRECTORY + File.separator + testId;
    int fileWriterBufferSizeInMB = 10;
    EmbeddingProvider embeddingProvider = null;
    if (!new File(sstablesOutputDirectory).mkdirs()) {
      throw new RuntimeException("Failed to create directory: " + sstablesOutputDirectory);
    }
    OfflineCommandsProcessor offlineCommandsProcessor = OfflineCommandsProcessor.getInstance();
    // begin session
    Triple<BeginOfflineSessionResponse, CommandContext, SchemaInfo> beginSessionResponse =
        beginSession(
            offlineCommandsProcessor,
            namespace,
            sstablesOutputDirectory,
            fileWriterBufferSizeInMB,
            embeddingProvider,
            isVectorTable);
    BeginOfflineSessionResponse beginOfflineSessionResponse = beginSessionResponse.getLeft();
    if (beginOfflineSessionResponse.errors() != null
        && !beginOfflineSessionResponse.errors().isEmpty()) {
      throw new RuntimeException(
          "Error while beginning session : " + beginOfflineSessionResponse.errors());
    }
    CommandContext commandContext = beginSessionResponse.getMiddle();
    SchemaInfo schemaInfo = beginSessionResponse.getRight();
    String createTableCQL = schemaInfo.createTableCQL();
    String expectedCreateCQL =
        """
                    CREATE TABLE IF NOT EXISTS "test_namespace"."test_collection_false"(
                     key tuple<tinyint,text>,
                     tx_id timeuuid,
                     doc_json text,
                     exist_keys set<text>,
                     array_size map<text, int>,
                     array_contains set<text>,
                     query_bool_values map<text, tinyint>,
                     query_dbl_values map<text, decimal>,
                     query_text_values map<text, text>,
                     query_timestamp_values map<text, timestamp>,
                     query_null_values set<text>,
                     PRIMARY KEY (key))
                     WITH comment =
                     '{"collection":{"name":"BeginOfflineSessionCommand","schema_version":1,"options":{"defaultId":{"type":"uuid"}}}}'
                    """;
    // assertThat(createTableCQL).isEqualTo(expectedCreateCQL);//TODO-SL fix assertion
    assertThat(createTableCQL).isNotNull();
    String tableName = "test_collection" + (isVectorTable ? "_true" : "_false");
    assertThat(createTableCQL)
        .startsWith("CREATE TABLE IF NOT EXISTS \"test_namespace\".\"" + tableName);
    assertThat(schemaInfo.keyspaceName()).isEqualTo("test_namespace");
    assertThat(schemaInfo.tableName())
        .isEqualTo("test_collection" + (isVectorTable ? "_true" : "_false"));
    List<String> indexCQLs = new ArrayList<>(schemaInfo.indexCQLs());
    assertThat(indexCQLs.size()).isEqualTo(isVectorTable ? 9 : 8);
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON \"test_namespace\".\"%s\" (exist_keys) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON \"test_namespace\".\"%s\" (entries(array_size)) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON \"test_namespace\".\"%s\" (array_contains) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON \"test_namespace\".\"%s\" (entries(query_bool_values)) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON \"test_namespace\".\"%s\" (entries(query_dbl_values)) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON \"test_namespace\".\"%s\" (entries(query_text_values)) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_timestamp_values ON \"test_namespace\".\"%s\" (entries(query_timestamp_values)) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    indexCQLs.remove(
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON \"test_namespace\".\"%s\" (query_null_values) USING 'StorageAttachedIndex'"
            .formatted(tableName, tableName));
    if (isVectorTable) {
      indexCQLs.remove(
          "CREATE CUSTOM INDEX IF NOT EXISTS test_collection_true_query_vector_value ON \"test_namespace\".\"test_collection_true\" (query_vector_value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': 'COSINE'}");
    }
    assertThat(indexCQLs.size()).isEqualTo(0);
    String sessionId = beginOfflineSessionResponse.sessionId();
    // load data
    List<JsonNode> jsonNodes = getRecords(includeVectorData);
    OfflineInsertManyResponse offlineInsertManyResponse =
        loadTestData(offlineCommandsProcessor, commandContext, sessionId, jsonNodes);
    boolean verifyVectorDataForNonVectorTable = !isVectorTable && includeVectorData;
    if (offlineInsertManyResponse.errors() != null
        && !offlineInsertManyResponse.errors().isEmpty()) {
      if (verifyVectorDataForNonVectorTable) {
        assertThat(offlineInsertManyResponse.errors().size()).isEqualTo(1);
        assertThat(offlineInsertManyResponse.errors().get(0).message())
            .contains("Vector search is not enabled for the collection %s".formatted(tableName));
        return;
      }
      throw new RuntimeException(
          "Error while inserting data : " + offlineInsertManyResponse.errors());
    }
    if (verifyVectorDataForNonVectorTable) {
      fail("Should have failed for vector data in non vector table");
    }
    // get statsus
    OfflineGetStatusResponse offlineGetStatusResponse =
        getStatus(offlineCommandsProcessor, commandContext, sessionId);
    if (offlineGetStatusResponse.errors() != null && !offlineGetStatusResponse.errors().isEmpty()) {
      throw new RuntimeException(
          "Error while getting status : " + offlineGetStatusResponse.errors());
    }
    assertEquals(sessionId, offlineGetStatusResponse.offlineWriterSessionStatus().sessionId());
    assertEquals(namespace, offlineGetStatusResponse.offlineWriterSessionStatus().keyspace());
    assertEquals(
        "test_collection_" + isVectorTable,
        offlineGetStatusResponse.offlineWriterSessionStatus().tableName());
    assertEquals(
        sstablesOutputDirectory,
        offlineGetStatusResponse.offlineWriterSessionStatus().ssTableOutputDirectory());
    assertEquals(
        fileWriterBufferSizeInMB,
        offlineGetStatusResponse.offlineWriterSessionStatus().fileWriterBufferSizeInMB());
    assertEquals(10, offlineGetStatusResponse.offlineWriterSessionStatus().insertsSucceeded());
    assertEquals(0, offlineGetStatusResponse.offlineWriterSessionStatus().insertsFailed());
    // it will be zero since the data is not flushed to disk yet
    assertEquals(
        0, offlineGetStatusResponse.offlineWriterSessionStatus().dataDirectorySizeInBytes());
    // end session
    EndOfflineSessionResponse endOfflineSessionResponse =
        endSession(offlineCommandsProcessor, commandContext, sessionId);
    if (endOfflineSessionResponse.errors() != null
        && !endOfflineSessionResponse.errors().isEmpty()) {
      throw new RuntimeException(
          "Error while ending session : " + endOfflineSessionResponse.errors());
    }
    assertEquals(sessionId, endOfflineSessionResponse.offlineWriterSessionStatus().sessionId());
    assertEquals(namespace, endOfflineSessionResponse.offlineWriterSessionStatus().keyspace());
    assertEquals(
        "test_collection_" + isVectorTable,
        endOfflineSessionResponse.offlineWriterSessionStatus().tableName());
    assertEquals(
        sstablesOutputDirectory,
        endOfflineSessionResponse.offlineWriterSessionStatus().ssTableOutputDirectory());
    assertEquals(
        fileWriterBufferSizeInMB,
        endOfflineSessionResponse.offlineWriterSessionStatus().fileWriterBufferSizeInMB());
    assertEquals(10, endOfflineSessionResponse.offlineWriterSessionStatus().insertsSucceeded());
    assertEquals(0, endOfflineSessionResponse.offlineWriterSessionStatus().insertsFailed());
    assertTrue(
        endOfflineSessionResponse.offlineWriterSessionStatus().dataDirectorySizeInBytes() > 0);
    // verify all files are created
    File[] files = new File(sstablesOutputDirectory).listFiles();
    if (files == null || files.length == 0) {
      throw new RuntimeException("No files created in directory: " + sstablesOutputDirectory);
    }
    boolean rowsDBFileFound = false;
    boolean filterDBFileFound = false;
    boolean partitionsFileFound = false;
    boolean dataFileFound = false;
    boolean digestCRC32FileFound = false;
    boolean compressionInfoFileFound = false;
    boolean statisticsFileFound = false;
    boolean tocFileFound = false;
    for (File file : files) {
      if (file.getName().endsWith("-bti-Rows.db")) {
        rowsDBFileFound = true;
      } else if (file.getName().endsWith("-bti-Filter.db")) {
        filterDBFileFound = true;
      } else if (file.getName().endsWith("-bti-Partitions.db")) {
        partitionsFileFound = true;
      } else if (file.getName().endsWith("-bti-Data.db")) {
        dataFileFound = true;
      } else if (file.getName().endsWith("-bti-Digest.crc32")) {
        digestCRC32FileFound = true;
      } else if (file.getName().endsWith("-bti-CompressionInfo.db")) {
        compressionInfoFileFound = true;
      } else if (file.getName().endsWith("-bti-Statistics.db")) {
        statisticsFileFound = true;
      } else if (file.getName().endsWith("-bti-TOC.txt")) {
        tocFileFound = true;
      } else {
        throw new RuntimeException("Unexpected file found: " + file.getName());
      }
    }
    assertTrue(rowsDBFileFound, "Rows.db file not found");
    assertTrue(filterDBFileFound, "Filter.db file not found");
    assertTrue(partitionsFileFound, "Partitions.db file not found");
    assertTrue(dataFileFound, "Data.db file not found");
    assertTrue(digestCRC32FileFound, "Digest.crc32 file not found");
    assertTrue(compressionInfoFileFound, "CompressionInfo.db file not found");
    assertTrue(statisticsFileFound, "Statistics.db file not found");
    assertTrue(tocFileFound, "TOC.txt file not found");
  }

  private EndOfflineSessionResponse endSession(
      OfflineCommandsProcessor offlineCommandsProcessor,
      CommandContext commandContext,
      String sessionId)
      throws ExecutionException, InterruptedException {
    return offlineCommandsProcessor.endSession(sessionId, commandContext);
  }

  private OfflineGetStatusResponse getStatus(
      OfflineCommandsProcessor offlineCommandsProcessor,
      CommandContext commandContext,
      String sessionId)
      throws ExecutionException, InterruptedException {
    return offlineCommandsProcessor.getStatus(commandContext, sessionId);
  }

  private List<JsonNode> getRecords(boolean isVector) throws IOException {
    List<JsonNode> jsonNodes = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                Objects.requireNonNull(
                    this.getClass()
                        .getResourceAsStream(
                            isVector ? "/10records_with_vector.jsonl" : "/10records.jsonl"))))) {
      String line;
      while ((line = reader.readLine()) != null) {
        jsonNodes.add(objectMapper.readTree(line));
      }
      return jsonNodes;
    }
  }

  private OfflineInsertManyResponse loadTestData(
      OfflineCommandsProcessor offlineCommandsProcessor,
      CommandContext commandContext,
      String sessionId,
      List<JsonNode> jsonNodes)
      throws ExecutionException, InterruptedException {
    return offlineCommandsProcessor.loadData(sessionId, commandContext, jsonNodes);
  }

  private Triple<BeginOfflineSessionResponse, CommandContext, SchemaInfo> beginSession(
      OfflineCommandsProcessor offlineCommandsProcessor,
      String namespace,
      String ssTablesOutputDirectory,
      int fileWriterBufferSizeInMB,
      EmbeddingProvider embeddingProvider,
      boolean includeVectorSearch)
      throws ExecutionException, InterruptedException {
    CreateCollectionCommand createCollectionCommand =
        buildCreateCollectionCommand(includeVectorSearch);
    return offlineCommandsProcessor.beginSession(
        createCollectionCommand,
        namespace,
        ssTablesOutputDirectory,
        fileWriterBufferSizeInMB,
        embeddingProvider);
  }

  private static CreateCollectionCommand buildCreateCollectionCommand(boolean includeVectorSearch) {
    String name = "test_collection" + "_" + includeVectorSearch;
    CreateCollectionCommand.Options.IdConfig idConfig =
        new CreateCollectionCommand.Options.IdConfig("uuid");
    CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig vectorizeConfig = null;
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig =
        includeVectorSearch
            ? new CreateCollectionCommand.Options.VectorSearchConfig(
                3, CollectionSettings.SimilarityFunction.COSINE.toString(), vectorizeConfig)
            : null;
    CreateCollectionCommand.Options.IndexingConfig indexingConfig = null;
    CreateCollectionCommand.Options options =
        new CreateCollectionCommand.Options(idConfig, vectorSearchConfig, indexingConfig);
    return new CreateCollectionCommand(name, options);
  }
}
