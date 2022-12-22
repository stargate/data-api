package io.stargate.sgv3.docsapi.api.configuration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.quarkus.jackson.ObjectMapperCustomizer;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

/** Configures the {@link ObjectMapper} instance that going to be injectable and used in the app. */
public class ObjectMapperConfiguration {

  /** Replaces the CDI producer for ObjectMapper built into Quarkus. */
  @Singleton
  @Produces
  ObjectMapper objectMapper(Instance<ObjectMapperCustomizer> customizers) {
    ObjectMapper mapper = createMapper();

    // apply all ObjectMapperCustomizer beans (incl. Quarkus)
    for (ObjectMapperCustomizer customizer : customizers) {
      customizer.customize(mapper);
    }

    return mapper;
  }

  private ObjectMapper createMapper() {
    // enabled:
    // case insensitive enums, so "before" will match to "BEFORE" in an enum
    return JsonMapper.builder()
        // important for retaining number accuracy!
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
        .build();
  }
}
