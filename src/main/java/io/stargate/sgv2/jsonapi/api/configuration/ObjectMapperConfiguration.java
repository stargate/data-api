package io.stargate.sgv2.jsonapi.api.configuration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.quarkus.jackson.ObjectMapperCustomizer;
import io.stargate.sgv2.jsonapi.api.model.command.serializer.CqlVectorSerializer;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/** Configures the {@link ObjectMapper} instance that going to be injectable and used in the app. */
public class ObjectMapperConfiguration {
  /** Replaces the CDI producer for ObjectMapper built into Quarkus. */
  @Singleton
  @Produces
  ObjectMapper objectMapper(
      DocumentLimitsConfig documentLimitsConfig, Instance<ObjectMapperCustomizer> customizers) {
    ObjectMapper mapper = createMapper(documentLimitsConfig);
    // apply all ObjectMapperCustomizer beans (incl. Quarkus)
    for (ObjectMapperCustomizer customizer : customizers) {
      customizer.customize(mapper);
    }

    // 14-Aug-2024, tatu: Our error handling assumes that by default we do fail on
    //    unrecognized properties: ensure it's not disabled
    mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    return mapper;
  }

  private ObjectMapper createMapper(DocumentLimitsConfig documentLimitsConfig) {
    int maxNumLen = documentLimitsConfig.maxNumberLength();

    // Number token limit handled by lower-level parser factory, need to construct first:
    JsonFactory jsonFactory =
        JsonFactory.builder()
            // Configure maximum number length (against DoS)
            .streamReadConstraints(
                StreamReadConstraints.builder().maxNumberLength(maxNumLen).build())
            // Enable fast(er) floating-point number handling
            .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
            .enable(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
            .enable(StreamWriteFeature.USE_FAST_DOUBLE_WRITER)
            // Verify uniqueness of JSON Object properties
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            // Include source snippet for easier troubleshooting on parse failures:
            .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
            /* 12-Dec-2023, tatu: Must not force use of Plain notation because
             *  that caused: https://github.com/stargate/jsonapi/issues/726
             *  where we can insert numbers that are not round-trippable
             *  as they get expanded from more compact Engineering/Scientific
             *  notation to longer Plain notation; effectively creating "unreadable"
             *  Documents.
             */
            .disable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .build();
    // For now, just one custom serializer to register
    SimpleModule cqlSerializersModule =
        new SimpleModule("CqlVectorSerializerModule").addSerializer(new CqlVectorSerializer());
    JsonMapper mapper =
        JsonMapper.builder(jsonFactory)
            // important for retaining number accuracy!
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            // case-insensitive enums, so "before" will match to "BEFORE" in an enum
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            // Add custom serializers/deserializers:
            .addModule(cqlSerializersModule)
            // And problem (error) handler too
            .addHandler(new CommandObjectMapperHandler())
            .build();
    return mapper;
  }
}
