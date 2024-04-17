package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.FileWriterSession;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineWriterSessionStatus;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public record OfflineGetStatusOperation(String sessionId) implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    FileWriterSession fileWriterSession =
        (FileWriterSession) queryExecutor.getCqlSessionCache().getSession(dataApiRequestInfo);
    if (fileWriterSession == null) {
      CommandResult commandResult =
          new CommandResult(
              List.of(
                  new CommandResult.Error(
                      "Session not found", null, null, Response.Status.NOT_FOUND)));
      return Uni.createFrom().item(() -> () -> commandResult);
    }
    OfflineWriterSessionStatus offlineWriterSessionStatus = fileWriterSession.getStatus();
    CommandResult commandResult =
        new CommandResult(
            Map.of(CommandStatus.OFFLINE_WRITER_SESSION_STATUS, offlineWriterSessionStatus));
    return Uni.createFrom().item(() -> () -> commandResult);
  }
}
