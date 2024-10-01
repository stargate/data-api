package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;

public class KeyspaceDriverExceptionHandler
    extends DefaultDriverExceptionHandler<KeyspaceSchemaObject> {}
