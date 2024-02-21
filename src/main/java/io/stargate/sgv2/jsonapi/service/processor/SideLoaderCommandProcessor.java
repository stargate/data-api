package io.stargate.sgv2.jsonapi.service.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.FileWriterSession;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.SideloaderConfigHelper;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.InsertManyCommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SideLoaderCommandProcessor {
  private final String namespace;
  private final String collection;
  private final OperationsConfig operationsConfig;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;
  private final MetricsConfig metricsConfig;
  private final CommandLevelLoggingConfig commandLevelLoggingConfig;

  public SideLoaderCommandProcessor(String namespace, String collection) {
    this.namespace = namespace;
    this.collection = collection;
    this.operationsConfig = SideloaderConfigHelper.buildOperationsConfig();
    ;
    this.meterRegistry = SideloaderConfigHelper.buildMeterRegistry();
    this.jsonApiMetricsConfig = SideloaderConfigHelper.buildJsonApiMetricsConfig();
    this.metricsConfig = SideloaderConfigHelper.buildMetricsConfig();
    this.commandLevelLoggingConfig = SideloaderConfigHelper.buildCommandLevelLoggingConfig();
  }

  public String beginWriterSession() {
    String sessionId = UUID.randomUUID().toString();
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(
            Optional.of(sessionId), new FileWriterParams(this.namespace, this.collection));
    CqlSession fileWriterSession =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, meterRegistry).getSession();
    if (fileWriterSession == null) {
      throw new RuntimeException("Can not create SSTable writer session");
    }
    return sessionId;
  }

  public SSTableWriterStatus insertDocuments(String sessionId, List<JsonNode> documents)
      throws ExecutionException, InterruptedException {
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(
            Optional.of(sessionId), new FileWriterParams(this.namespace, this.collection));
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, meterRegistry);
    QueryExecutor queryExecutor = new QueryExecutor(cqlSessionCache, operationsConfig);
    MeteredCommandProcessor meteredCommandProcessor =
        getMeteredCommandProcessor(queryExecutor, meterRegistry, dataApiRequestInfo);
    // Build command and command context
    FileWriterSession fileWriterSession = (FileWriterSession) cqlSessionCache.getSession();
    CommandContext commandContext =
        new CommandContext(fileWriterSession.getNamespace(), fileWriterSession.getCollection());
    InsertManyCommand.Options options = new InsertManyCommand.Options(false);
    Command insertManyCommand = new InsertManyCommand(documents, options);
    // Execute command
    CommandResult commandResult =
        meteredCommandProcessor
            .processCommand(commandContext, insertManyCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    // Convert result to SSTableWriterStatus
    return toSSTableWriterStatus(commandContext, commandResult);
  }

  private MeteredCommandProcessor getMeteredCommandProcessor(
      QueryExecutor queryExecutor,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo) {
    CommandResolverService commandResolverService = buildCommandResolverService();
    CommandProcessor commandProcessor = new CommandProcessor(queryExecutor, commandResolverService);
    return new MeteredCommandProcessor(
        commandProcessor,
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        metricsConfig,
        commandLevelLoggingConfig);
  }

  private static SSTableWriterStatus toSSTableWriterStatus(
      CommandContext commandContext, CommandResult commandResult) {
    return new SSTableWriterStatus(commandContext.namespace(), commandContext.collection());
  }

  public SSTableWriterStatus getWriterStatus(String sessionId) {
    FileWriterSession fileWriterSession =
        (FileWriterSession) new CQLSessionCache(null, null, null).getSession();
    return fileWriterSession.getStatus();
  }

  public void endWriterSession(String sessionId) {
    CqlSession fileWriterSession = new CQLSessionCache(null, null, null).getSession();
    fileWriterSession.close();
  }

  private static CommandResolverService buildCommandResolverService() {
    ObjectMapper objectMapper = new ObjectMapper();
    DocumentLimitsConfig documentLimitsConfig = SideloaderConfigHelper.buildDocumentLimitsConfig();
    Shredder shredder = new Shredder(objectMapper, documentLimitsConfig);
    return new CommandResolverService(
        List.of(new InsertManyCommandResolver(shredder, objectMapper)));
  }
}
