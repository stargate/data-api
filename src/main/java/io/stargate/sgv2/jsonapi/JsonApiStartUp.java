package io.stargate.sgv2.jsonapi;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilder;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonApiStartUp {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonApiStartUp.class);
  private final DebugModeConfig config;
  private final OperationsConfig operationsConfig;
  public static final String CASSANDRA = "cassandra";

  private static final String CREATE_KEYSPACE_CQL =
      "CREATE KEYSPACE IF NOT EXISTS startupks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};";
  private static final String CREATE_TABLE_CQL =
      "CREATE TABLE IF NOT EXISTS startupks.startuptable (id int, field text, PRIMARY KEY (id));";
  private static final String CREATE_INDEX_CQL =
      "CREATE CUSTOM INDEX startup_sai_idx ON startupks.startuptable (field) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};";
  private static final String DROP_INDEX_CQL = "DROP INDEX IF EXISTS startupks.startup_sai_idx;";
  private static final String DROP_TABLE_CQL = "DROP TABLE IF EXISTS startupks.startuptable;";
  private static final String DROP_KEYSPACE_CQL = "DROP KEYSPACE IF EXISTS startupks;";
  private static final String DEFAULT_TENANT = "default_tenant";

  @Inject
  public JsonApiStartUp(DebugModeConfig config, OperationsConfig operationsConfig) {
    this.config = config;
    this.operationsConfig = operationsConfig;
  }

  void onStart(@Observes StartupEvent ev) {
    LOGGER.info(String.format("DEBUG mode Enabled: %s", config.enabled()));
    // only check for local cassandra, see if SAI is enabled
    if (CASSANDRA.equals(operationsConfig.databaseConfig().type())) {
      List<InetSocketAddress> seeds =
          Objects.requireNonNull(operationsConfig.databaseConfig().cassandraEndPoints()).stream()
              .map(
                  host ->
                      new InetSocketAddress(
                          host, operationsConfig.databaseConfig().cassandraPort()))
              .collect(Collectors.toList());

      final CqlSession cqlSessionForStartup =
          new TenantAwareCqlSessionBuilder(DEFAULT_TENANT)
              .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
              .addContactPoints(seeds)
              .withAuthCredentials(
                  Objects.requireNonNull(operationsConfig.databaseConfig().userName()),
                  Objects.requireNonNull(operationsConfig.databaseConfig().password()))
              .build();

      // create a test startup keyspace
      cqlSessionForStartup.execute(SimpleStatement.newInstance(CREATE_KEYSPACE_CQL));
      // create a test startup table
      cqlSessionForStartup.execute(SimpleStatement.newInstance(CREATE_TABLE_CQL));
      try {
        // create a test index, see if SAI is enabled
        cqlSessionForStartup.execute(SimpleStatement.newInstance(CREATE_INDEX_CQL));
      } catch (InvalidConfigurationInQueryException invalidConfigurationInQueryException) {
        cqlSessionForStartup.execute(SimpleStatement.newInstance(DROP_TABLE_CQL));
        cqlSessionForStartup.execute(SimpleStatement.newInstance(DROP_KEYSPACE_CQL));
        stopJSONAPI();
      }
      cqlSessionForStartup.execute(SimpleStatement.newInstance(DROP_INDEX_CQL));
      cqlSessionForStartup.execute(SimpleStatement.newInstance(DROP_TABLE_CQL));
      cqlSessionForStartup.execute(SimpleStatement.newInstance(DROP_KEYSPACE_CQL));
    }
  }

  private void stopJSONAPI() {
    LOGGER.warn(
        "Your Cassandra Persistence does not support Storage Attached Indexing (SAI), fail to start the JSONAPI");
    Quarkus.asyncExit();
  }
}
