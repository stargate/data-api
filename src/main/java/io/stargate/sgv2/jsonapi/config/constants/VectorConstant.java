package io.stargate.sgv2.jsonapi.config.constants;

import io.smallrye.config.ConfigMapping;
import java.util.Set;

@ConfigMapping(prefix = "stargate.jsonapi.vector")
public interface VectorConstant {
  /*
  Supported Source Models for Vector Index in Cassandra
   */
  Set<String> SUPPORTED_SOURCES =
      Set.of(
          "ada002", "openai_v3_small", "openai_v3_large", "bert", "gecko", "nv_qa_4", "cohere_v3");
}
