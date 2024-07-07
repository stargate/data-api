package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import io.stargate.sgv2.jsonapi.service.operation.model.Operation;

/**
 * Base for any operations that works with CQL Tables with rows, rather than Collections of
 * Documents
 */
abstract class TableOperation implements Operation {}
