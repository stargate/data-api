package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Polymorphic operation payload for {@link AlterCollectionCommand}. Each operation is represented
 * by a record implementing this interface; Jackson selects the concrete subtype by the wrapper key
 * (e.g. {@code "enableLexical"}). Mirrors {@link AlterTableOperation}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({@JsonSubTypes.Type(value = AlterCollectionOperationImpl.EnableLexical.class)})
public sealed interface AlterCollectionOperation
    permits AlterCollectionOperationImpl.EnableLexical {}
