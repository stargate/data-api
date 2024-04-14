package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.junit.jupiter.api.Assertions.*;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BeginOfflineSessionCommandTest {

  @Test
  void testBeginOfflineSessionCommand() {
    String idType = "uuid";
    int fileWriterBufferSizeInMB = 20;
    String namespace = "namespace1";
    String collectionName = "collection1";
    String ssTableOutputDirectory = "ssTableOutputDirectory";
    int vectorDimension = 1536;
    String similarityFunction = CollectionSettings.SimilarityFunction.EUCLIDEAN.toString();
    CreateCollectionCommand.Options.IdConfig idConfig =
        new CreateCollectionCommand.Options.IdConfig(idType);
    CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig vectorizeConfig = null;
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig =
        new CreateCollectionCommand.Options.VectorSearchConfig(
            vectorDimension, similarityFunction, vectorizeConfig);
    CreateCollectionCommand.Options.IndexingConfig indexingConfig = null;
    CreateCollectionCommand.Options createCollectionCommandOptions =
        new CreateCollectionCommand.Options(idConfig, vectorSearchConfig, indexingConfig);
    CreateCollectionCommand createCollectionCommand =
        new CreateCollectionCommand(collectionName, createCollectionCommandOptions);
    BeginOfflineSessionCommand beginOfflineSessionCommand =
        new BeginOfflineSessionCommand(
            namespace,
            createCollectionCommand,
            ssTableOutputDirectory,
            null,
            fileWriterBufferSizeInMB);

    verifySessionId(beginOfflineSessionCommand);
    verifyFileWriterParams(
        beginOfflineSessionCommand,
        namespace,
        collectionName,
        ssTableOutputDirectory,
        fileWriterBufferSizeInMB,
        false);
    verifyCollectionSettings(
        beginOfflineSessionCommand,
        collectionName,
        false,
        idType,
        vectorDimension,
        similarityFunction);
  }

  private void verifyCollectionSettings(
      BeginOfflineSessionCommand beginOfflineSessionCommand,
      String collectionName,
      boolean isVectorNull,
      String idType,
      int vectorDimension,
      String similarityFunction) {
    assertEquals(
        collectionName, beginOfflineSessionCommand.getCollectionSettings().collectionName());
    assertEquals(
        idType, beginOfflineSessionCommand.getCollectionSettings().idConfig().idType().toString());
    if (isVectorNull) {
      assertNull(beginOfflineSessionCommand.getCollectionSettings().vectorConfig());
    } else {
      assertTrue(beginOfflineSessionCommand.getCollectionSettings().vectorConfig().vectorEnabled());
      assertEquals(
          vectorDimension,
          beginOfflineSessionCommand.getCollectionSettings().vectorConfig().vectorSize());
      assertEquals(
          similarityFunction,
          beginOfflineSessionCommand
              .getCollectionSettings()
              .vectorConfig()
              .similarityFunction()
              .toString());
      assertNull(
          beginOfflineSessionCommand.getCollectionSettings().vectorConfig().vectorizeConfig());
    }
    assertNull(beginOfflineSessionCommand.getCollectionSettings().indexingConfig());
  }

  private void verifyFileWriterParams(
      BeginOfflineSessionCommand beginOfflineSessionCommand,
      String namespace,
      String collectionName,
      String ssTableOutputDirectory,
      int fileWriterBufferSizeInMB,
      boolean isVectorNull) {
    // Verify FileWriterParams
    assertEquals(namespace, beginOfflineSessionCommand.getFileWriterParams().keyspaceName());
    assertEquals(collectionName, beginOfflineSessionCommand.getFileWriterParams().tableName());
    assertEquals(
        ssTableOutputDirectory,
        beginOfflineSessionCommand.getFileWriterParams().ssTableOutputDirectory());
    assertEquals(
        fileWriterBufferSizeInMB,
        beginOfflineSessionCommand.getFileWriterParams().fileWriterBufferSizeInMB());
    // TODO-SL verify CQLs
    if (isVectorNull) {
      assertFalse(beginOfflineSessionCommand.getFileWriterParams().vectorEnabled());
    } else {
      assertTrue(beginOfflineSessionCommand.getFileWriterParams().vectorEnabled());
    }
  }

  private void verifySessionId(BeginOfflineSessionCommand beginOfflineSessionCommand) {
    assertInstanceOf(String.class, beginOfflineSessionCommand.getSessionId());
    assertEquals(
        beginOfflineSessionCommand.getSessionId(),
        UUID.fromString(beginOfflineSessionCommand.getSessionId()).toString());
  }

  @Test
  void testBeginOfflineSessionCommandNoVector() {
    String idType = "uuid";
    int fileWriterBufferSizeInMB = 20;
    String namespace = "namespace1";
    String collectionName = "collection1";
    String ssTableOutputDirectory = "ssTableOutputDirectory";

    CreateCollectionCommand.Options.IdConfig idConfig =
        new CreateCollectionCommand.Options.IdConfig(idType);
    CreateCollectionCommand.Options.IndexingConfig indexingConfig = null;
    CreateCollectionCommand.Options createCollectionCommandOptions =
        new CreateCollectionCommand.Options(idConfig, null, indexingConfig);
    CreateCollectionCommand createCollectionCommand =
        new CreateCollectionCommand(collectionName, createCollectionCommandOptions);
    BeginOfflineSessionCommand beginOfflineSessionCommand =
        new BeginOfflineSessionCommand(
            namespace,
            createCollectionCommand,
            ssTableOutputDirectory,
            null,
            fileWriterBufferSizeInMB);

    verifySessionId(beginOfflineSessionCommand);
    verifyFileWriterParams(
        beginOfflineSessionCommand,
        namespace,
        collectionName,
        ssTableOutputDirectory,
        fileWriterBufferSizeInMB,
        true);
    verifyCollectionSettings(beginOfflineSessionCommand, collectionName, true, idType, 0, null);
  }
}
