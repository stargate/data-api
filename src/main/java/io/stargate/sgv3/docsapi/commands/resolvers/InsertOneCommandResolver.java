package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.InsertOneCommand;
import io.stargate.sgv3.docsapi.operations.InsertOperation;
import io.stargate.sgv3.docsapi.operations.Operation;
import io.stargate.sgv3.docsapi.shredding.Shredder;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;

/** Resolve the {@link InsertOneCommand} */
public class InsertOneCommandResolver implements CommandResolver<InsertOneCommand> {

  @Override
  public Operation resolveCommand(CommandContext commandContext, InsertOneCommand command) {

    // insertOne is probably always going to be simple, but here we need to shred the document
    // because Operations
    // do not touch raw JSON

    WritableShreddedDocument doc = new Shredder().shred(command.document);
    return new InsertOperation(commandContext, doc);
  }
}
