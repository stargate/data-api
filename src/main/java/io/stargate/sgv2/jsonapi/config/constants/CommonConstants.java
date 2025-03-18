package io.stargate.sgv2.jsonapi.config.constants;

/** Contains common constants shared in different parts of the codebase. */
public interface CommonConstants {
  /** Common service configuration constants shared between vector and reranking */
  interface ServiceConfig {
    String SERVICE = "service";
    String PROVIDER = "provider";
    String MODEL_NAME = "modelName";
    String AUTHENTICATION = "authentication";
    String PARAMETERS = "parameters";
  }
}
