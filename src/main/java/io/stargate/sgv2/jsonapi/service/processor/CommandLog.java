package io.stargate.sgv2.jsonapi.service.processor;

import io.stargate.sgv2.jsonapi.api.model.command.CommandError;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import java.util.List;

/**
 * Command log is used to store information about command execution. It is used to log command when
 * the appropriate flags are set as per {@link
 * io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig}
 */
public record CommandLog(
    String commandName,
    Tenant tenant,
    String namespaceName,
    String collectionName, // leave as collectionName for logging analysis
    String schemaType,
    String documentsReceived,
    String documentsReturned,
    List<CommandError> errorList) {}
