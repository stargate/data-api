package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.OFFLINE_WRITER;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineBeginWriterCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

public record OfflineBeginWriterOperation(
    CommandContext ctx,
    OfflineBeginWriterCommand command,
    Shredder shredder,
    ObjectMapper objectMapper)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    String sessionId = UUID.randomUUID().toString();
    String createTableCQL =
        CreateCollectionOperation.forCQL(
                ctx.collectionSettings().vectorEnabled(),
                ctx.collectionSettings().vectorSize(),
                "") // TODO-SL fix comments field
            .getCreateTable(ctx.namespace(), ctx.collection())
            .getQuery();
    String insertStatementCQL =
        InsertOperation.forCQL(new CommandContext(ctx.namespace(), ctx.collection()))
            .buildInsertQuery(
                ctx.collectionSettings()
                    .vectorEnabled()); // TODO-SL add conditionalInsert to method
    FileWriterParams fileWriterParams =
        new FileWriterParams(
            ctx.namespace(),
            ctx.collection(),
            command.ssTableOutputDirectory(),
            createTableCQL,
            insertStatementCQL);
    DataApiRequestInfo dataApiRequestInfo =
        new DataApiRequestInfo(Optional.of(sessionId), fileWriterParams);
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", OFFLINE_WRITER)
            .build();
    OperationsConfig operationsConfig = smallRyeConfig.getConfigMapping(OperationsConfig.class);
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(dataApiRequestInfo, operationsConfig, new SimpleMeterRegistry());
    cqlSessionCache.getSession();
    CommandResult commandResult =
        new CommandResult(Map.of(CommandStatus.OFFLINE_WRITER_SESSION_ID, sessionId.toString()));
    return Uni.createFrom().item(() -> () -> commandResult);
  }
}
