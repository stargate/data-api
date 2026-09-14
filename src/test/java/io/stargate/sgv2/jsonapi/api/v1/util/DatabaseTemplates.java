package io.stargate.sgv2.jsonapi.api.v1.util;

/** Templates command that run against a Database */
public class DatabaseTemplates extends TemplateRunner {

  private DataApiDatabaseCommandSender sender;

  public DatabaseTemplates(DataApiDatabaseCommandSender sender) {
    this.sender = sender;
  }

  // ===================================================================================================================
  // DDL - TABLES
  // ===================================================================================================================

  public DataApiResponseValidator createKeyspace(String keyspaceName) {
    var json =
            """
            {
                "name": "%s"
            }
            """
            .formatted(keyspaceName);
    return sender.postCreateKeyspace(json);
  }

  public DataApiResponseValidator dropKeyspace(String keyspaceName) {
    var json =
            """
            {
                "name": "%s"
            }
            """
            .formatted(keyspaceName);
    return sender.postDropKeyspace(json);
  }
}
