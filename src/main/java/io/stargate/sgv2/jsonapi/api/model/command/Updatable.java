package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;

/*
 * All the commands that supports update clause will have to implement this.
 */
public interface Updatable {
  UpdateClause updateClause();
}
