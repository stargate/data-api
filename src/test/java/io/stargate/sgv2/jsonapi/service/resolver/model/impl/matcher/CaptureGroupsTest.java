// package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;
//
// import static org.assertj.core.api.Assertions.assertThat;
//
// import com.fasterxml.jackson.databind.ObjectMapper;
// import io.quarkus.test.junit.QuarkusTest;
// import io.quarkus.test.junit.TestProfile;
// import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
// import io.stargate.sgv2.jsonapi.api.model.command.Command;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperation;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
// import jakarta.inject.Inject;
// import java.util.AbstractMap;
// import java.util.List;
// import java.util.Map;
// import org.junit.jupiter.api.Nested;
// import org.junit.jupiter.api.Test;
//
// @QuarkusTest
// @TestProfile(NoGlobalResourcesTestProfile.Impl.class)
// public class CaptureGroupsTest {
//  @Inject ObjectMapper objectMapper;
//
//  @Nested
//  class CaptureGroupCreator {
//
//    @Test
//    public void getGroupTest() throws Exception {
//      String json =
//          """
//                    {
//                      "findOne": {
//                        "sort" : {"user.name" : 1, "user.age" : -1},
//                        "filter": {"username": "aaron"}
//                      }
//                    }
//                    """;
//
//      Command result = objectMapper.readValue(json, Command.class);
//      CaptureGroups captureGroups = new CaptureGroups(result);
//
//      CaptureGroup captureGroup = captureGroups.getGroup("TEST");
//      assertThat(captureGroup.captures().keySet()).hasSize(0);
//
//      List<FilterOperation> filters =
//          List.of(
//              new ValueComparisonOperation(
//                  ValueComparisonOperator.EQ, new JsonLiteral("abc", JsonType.STRING)));
//      Map.Entry<String, List<FilterOperation>> expected =
//          new AbstractMap.SimpleEntry<String, List<FilterOperation>>("test", filters);
//      captureGroup.withCapture("test", filters);
//
//      captureGroup = captureGroups.getGroup("TEST");
//      assertThat(captureGroup.captures().entrySet()).contains(expected);
//    }
//  }
// }
