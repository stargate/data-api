package io.stargate.sgv2.jsonapi.config.constants;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;

/** Default values applied to table descriptions. */
public interface TableDescDefaults {

  /**
   * Defaults for {@link
   * io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand.CreateIndexCommandOptions}.
   */
  interface CreateIndexOptionsDefaults {
    boolean IF_NOT_EXISTS = false;

    // For use in @Schema decorators
    interface Constants {
      String IF_NOT_EXISTS = "false";
    }
  }

  /**
   * Defaults for {@link
   * io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand.CreateVectorIndexCommandOptions}.
   */
  interface CreateVectorIndexOptionsDefaults {
    boolean IF_NOT_EXISTS = false;

    // For use in @Schema decorators
    interface Constants {
      String IF_NOT_EXISTS = "false";
    }
  }

  /** Defaults for {@link RegularIndexDefinitionDesc.RegularIndexDescOptions}. */
  interface RegularIndexDescDefaults {
    boolean ASCII = false;
    boolean CASE_SENSITIVE = true;
    boolean NORMALIZE = false;

    // For use in @Schema decorators
    interface Constants {
      String ASCII = "false";
      String CASE_SENSITIVE = "true";
      String NORMALIZE = "false";
    }
  }
}
