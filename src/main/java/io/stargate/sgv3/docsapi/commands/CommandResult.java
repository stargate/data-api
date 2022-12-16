package io.stargate.sgv3.docsapi.commands;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause.Projectable;
import io.stargate.sgv3.docsapi.operations.OperationResult;
import io.stargate.sgv3.docsapi.shredding.IDShreddedDocument;
import io.stargate.sgv3.docsapi.shredding.Shredder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POJO object (data no behavior) that has the result of running a command, either documents, list
 * of documents modified, or errors.
 *
 * <p>This class is part of the Command layer and is the bridge from the internal Command back to
 * the Message layer.
 *
 * <p>Because it is in the Command layer this is where we de-shred and do the Projection of what
 * fields we want from the document.
 */
public class CommandResult {
  public final List<JsonNode> docs;
  // leaving aas nullable string because I think that is easier when converting to JSON
  public final String nextPageState;

  /**
   * HACK: probably, the value of a status field can (i think) be either string, number, boolean, or
   * list of strings needs to be determined in spec. e.g. insertIds: ["1","2","3"] or "matchDocs": 3
   */
  public final Map<String, Object> status;

  public final List<Exception> errors;

  public CommandResult(
      List<JsonNode> docs,
      String nextPageState,
      Map<String, Object> status,
      List<Exception> errors) {
    this.docs = docs;
    this.nextPageState = nextPageState;
    this.status = status;
    this.errors = errors;
  }

  /**
   * Translate from internal Operation to Command, which may involve de-shredding and doing the
   * Projection
   *
   * @param command
   * @param opResult
   * @return
   */
  public static Uni<CommandResult> fromOperationResult(Command command, OperationResult opResult) {
    // This is where we would de-shred the document and do the projection if one is needed.

    if (!opResult.errors.isEmpty()) {
      // TODO Handle errors, there should ony be errors, also Jackson does not like the error
      // objects
      return Uni.createFrom().item(new CommandResult(List.of(), null, Map.of(), opResult.errors));
    }

    List<JsonNode> docs;
    String pageState;
    if (opResult.docs.isEmpty()) {
      docs = List.of();
      pageState = null;
    } else {
      if (!(command instanceof Projectable)) {
        throw new RuntimeException(
            String.format(
                "Invalid state, commandResult has documents but is not Projectable command = %s",
                command));
      }
      var projectable = (Projectable) command;
      // We have documents to add to the result, there could also be status info
      docs = new Shredder().deshred(projectable.getProjection(), opResult.docs);
      pageState = opResult.nextPageState.orElse(null);
    }

    // This is the place to map more structured operation results into the key-value sort of thing
    // commands use to return results, put another way. Its the only place "insertedIds" string
    // should be :)
    Map<String, Object> status = new HashMap<>();
    if (command instanceof SchemaModificationCommand) {
      status.put("ok", 1);
    } else {
      opResult.matchedCount.ifPresent(c -> status.put("matchedCount", c));
      if (!opResult.insertedIds.isEmpty()) {
        status.put("insertedIds", IDShreddedDocument.toIdStrings(opResult.insertedIds));
      }
      if (!opResult.updatedIds.isEmpty()) {
        status.put("updatedIds", IDShreddedDocument.toIdStrings(opResult.updatedIds));
      }
    }

    return Uni.createFrom().item(new CommandResult(docs, pageState, status, List.of()));
  }
}
