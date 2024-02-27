package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.BeginOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.Map;
import java.util.function.Supplier;

public record BeginOfflineSessionOperation(
    CommandContext ctx,
    BeginOfflineSessionCommand command,
    Shredder shredder,
    ObjectMapper objectMapper)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    CqlSession session = queryExecutor.getCqlSessionCache().getSession(true);
    // TODO-SL check session
    CommandResult commandResult =
        new CommandResult(Map.of(CommandStatus.OFFLINE_WRITER_SESSION_ID, command.getSessionId()));
    return Uni.createFrom().item(() -> () -> commandResult);
  }
}
