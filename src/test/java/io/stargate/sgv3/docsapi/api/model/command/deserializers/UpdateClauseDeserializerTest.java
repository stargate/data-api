package io.stargate.sgv3.docsapi.api.model.command.deserializers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation.UpdateValue;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation.UpdateValue.ValueType;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv3.docsapi.exception.DocsException;
import java.math.BigDecimal;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateClauseDeserializerTest {

  @Inject ObjectMapper objectMapper;

  @Nested
  class Deserialize {

    @Test
    public void happyPath() throws Exception {
      String json = """
                    {"username": "aaron"}
                    """;

      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      final UpdateOperation operation =
          new UpdateOperation<>(
              "username", UpdateOperator.SET, new UpdateValue<>("aaron", ValueType.STRING));
      assertThat(updateClause).isNotNull();
      assertThat(updateClause.updateOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void happyPathBsonOperation() throws Exception {
      String json =
          """
                    {"$set" : {"username": "aaron", "number" : 40}}
                    """;

      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      final UpdateOperation operation =
          new UpdateOperation<>(
              "username", UpdateOperator.SET, new UpdateValue<>("aaron", ValueType.STRING));
      assertThat(updateClause).isNotNull();
      assertThat(updateClause.updateOperations()).hasSize(2).contains(operation);
    }

    @Test
    public void mustHandleNull() throws Exception {
      String json = "null";

      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause).isNull();
    }

    @Test
    public void mustHandleEmpty() throws Exception {
      String json = """
                    {}
                    """;

      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.updateOperations()).hasSize(0);
    }

    @Test
    public void mustHandleString() throws Exception {
      String json = """
                    {"username": "aaron"}
                    """;
      final UpdateOperation operation =
          new UpdateOperation<>(
              "username", UpdateOperator.SET, new UpdateValue<>("aaron", ValueType.STRING));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.updateOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleNumber() throws Exception {
      String json = """
                    {"numberType": 40}
                    """;
      final UpdateOperation operation =
          new UpdateOperation<>(
              "numberType",
              UpdateOperator.SET,
              new UpdateValue<>(new BigDecimal(40), ValueType.NUMBER));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.updateOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void mustHandleBoolean() throws Exception {
      String json = """
                    {"boolType": true}
                    """;
      final UpdateOperation operation =
          new UpdateOperation<>(
              "boolType", UpdateOperator.SET, new UpdateValue<>(true, ValueType.BOOLEAN));
      UpdateClause updateClause = objectMapper.readValue(json, UpdateClause.class);
      assertThat(updateClause.updateOperations()).hasSize(1).contains(operation);
    }

    @Test
    public void unsupportedFilterTypes() {
      String json = """
                    {"boolType": ["a"]}
                    """;

      Throwable throwable = catchThrowable(() -> objectMapper.readValue(json, UpdateClause.class));
      assertThat(throwable).isInstanceOf(DocsException.class);
    }
  }
}
