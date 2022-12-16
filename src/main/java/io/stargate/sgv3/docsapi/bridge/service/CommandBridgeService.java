/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv3.docsapi.bridge.service;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.CommandResult;
import io.stargate.sgv3.docsapi.commands.resolvers.CommandResolver;
import io.stargate.sgv3.docsapi.commands.resolvers.Resolvers;
import io.stargate.sgv3.docsapi.operations.Operation;
import io.stargate.sgv3.docsapi.operations.OperationResult;
import io.stargate.sgv3.docsapi.service.OperationExecutor;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class CommandBridgeService {

  @Inject OperationExecutor operationExecutor;

  public <T extends Command> Uni<CommandResult> processCommand(
      CommandContext commandContext, T command) {

    // assert command.valid() : "Command must be validated";

    // resolver works our how to process the command.
    CommandResolver<T> resolver = Resolvers.resolverForCommand(command);

    // Get the actual operation we need to process the command, e.g. findById
    // Shredding happens in here.
    Operation operation = resolver.resolveCommand(commandContext, command);

    // Run the operation against the DB
    Uni<OperationResult> opResult = operationExecutor.executeOperation(operation);

    // Now need to de-shredd, happens when building the command result.
    return opResult.onItem().transformToUni(op -> CommandResult.fromOperationResult(command, op));
  }
}
