package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Interface that Commands that only take "empty" Command (that is, do not expose options but should
 * allow empty JSON Object nonetheless) should implement. <br>
 * NOTE: if {@code Command} starts accepting options, it should NO LONGER implement this interface
 * as combination will not work (options field will not be deserialized).
 */
@JsonIgnoreProperties({"options"})
public interface NoOptionsCommand {}
