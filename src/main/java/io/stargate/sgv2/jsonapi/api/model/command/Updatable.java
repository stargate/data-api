package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;

public interface Updatable {
  UpdateClause updateClause();
}
