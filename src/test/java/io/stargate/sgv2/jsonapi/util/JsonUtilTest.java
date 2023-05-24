package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Date;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class JsonUtilTest {
  @Inject ObjectMapper mapper;

  @Nested
  class Equality {
    @Test
    public void testScalarOrderedEquality() {
      assertThat(equalsOrdered("1", "1")).isTrue();
      assertThat(equalsOrdered("true", "true")).isTrue();
      assertThat(equalsOrdered("\"text\"", "\"text\"")).isTrue();

      assertThat(equalsOrdered("1", "2")).isFalse();
      assertThat(equalsOrdered("true", "false")).isFalse();
      assertThat(equalsOrdered("\"abc\"", "\"bcd\"")).isFalse();

      assertThat(equalsOrdered("0", "true")).isFalse();
    }

    @Test
    public void testArrayOrderedEquality() {
      assertThat(equalsOrdered("[ 1 ]", "[ 1 ]")).isTrue();

      assertThat(equalsOrdered("[1, 2]", "[2, 1]")).isFalse();
      assertThat(equalsOrdered("[1, 2]", "[1]")).isFalse();
    }

    @Test
    public void testObjectOrderedEquality() {
      assertThat(equalsOrdered("{ \"a\":1, \"b\":2 }", "{ \"a\":1, \"b\":2 }")).isTrue();
      assertThat(equalsOrdered("[{ \"a\":1, \"b\":2 }]", "[{ \"a\":1, \"b\":2 }]")).isTrue();
      assertThat(
              equalsOrdered(
                  "{ \"subdoc\" : { \"x\":0, \"y\":5 } }", "{ \"subdoc\" : { \"x\":0, \"y\":5 } }"))
          .isTrue();

      // The only interesting test; that for Objects: order of property must match too
      assertThat(equalsOrdered("{ \"a\":1, \"b\":2 }", "{ \"b\":2, \"a\":1 }")).isFalse();
      assertThat(equalsOrdered("[{ \"a\":1, \"b\":2 }]", "[{ \"b\":2, \"a\":1 }]")).isFalse();
      assertThat(
              equalsOrdered(
                  "{ \"subdoc\" : { \"x\":0 } }", "{ \"subdoc\" : { \"x\":0, \"y\":5 } }"))
          .isFalse();
      assertThat(
              equalsOrdered(
                  "{ \"subdoc\" : { \"y\":5, \"x\":0 } }", "{ \"subdoc\" : { \"x\":0, \"y\":5 } }"))
          .isFalse();
    }

    boolean equalsOrdered(String json1, String json2) {
      try {
        return JsonUtil.equalsOrdered(mapper.readTree(json1), mapper.readTree(json2));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Nested
  class EJSON {
    @Test
    public void maybeEJSONValue() throws Exception {
      // First valid one
      assertThat(JsonUtil.looksLikeEJsonValue(mapper.readTree("{\"$date\":123}"))).isTrue();

      // And then invalid but not too obviously so (unrecognized, wrong value type);
      // ones that "should be" EJSON values and are not legit JSON API values otherwise
      assertThat(JsonUtil.looksLikeEJsonValue(mapper.readTree("{\"$unknown\":123}"))).isTrue();
      assertThat(JsonUtil.looksLikeEJsonValue(mapper.readTree("{\"$date\": false}"))).isTrue();

      // But also things that are not
      assertThat(JsonUtil.looksLikeEJsonValue(mapper.readTree("{\"$date\":123, \"x\":3}")))
          .isFalse();
      assertThat(JsonUtil.looksLikeEJsonValue(mapper.readTree("\"$date\""))).isFalse();
      assertThat(JsonUtil.looksLikeEJsonValue(mapper.readTree("123456789"))).isFalse();
    }

    @Test
    public void validEJSONDate() {
      ObjectNode ob = mapper.createObjectNode();
      final long ts = 123456L;
      ob.put("$date", ts);
      Date dt = JsonUtil.extractEJsonDate(ob, "/");
      assertThat(dt).isEqualTo(new Date(ts));
    }

    @Test
    public void invalidEJSONDate() {
      ObjectNode ob = mapper.createObjectNode();
      ob.put("$date", "foobar");
      Throwable t = catchThrowable(() -> JsonUtil.extractEJsonDate(ob, "/"));
      assertThat(t)
          .isNotNull()
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.SHRED_BAD_EJSON_VALUE)
          .hasMessage(
              "Bad EJSON value: Date ($date) needs to have NUMBER value, has STRING (path '/')");
    }
  }
}
