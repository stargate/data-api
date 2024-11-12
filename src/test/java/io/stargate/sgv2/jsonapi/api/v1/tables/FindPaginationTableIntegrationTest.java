package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.KeyValueTable100Scenario;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindPaginationTableIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "findPaginationTableIntegrationTest";
  private static final KeyValueTable100Scenario SCENARIO =
      new KeyValueTable100Scenario(keyspaceName, TABLE_NAME);

  @BeforeAll
  public final void createScenario() {
    SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    SCENARIO.drop();
  }

  @Test
  public void findWithPageState() {

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(Map.of(), List.of())
        .wasSuccessful()
        .hasNextPageState();
  }
}
