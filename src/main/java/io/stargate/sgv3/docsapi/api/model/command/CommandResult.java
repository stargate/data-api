package io.stargate.sgv3.docsapi.api.model.command;

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
public record CommandResult() {

  // TODO implement by spec, ensure docs are updated

}
