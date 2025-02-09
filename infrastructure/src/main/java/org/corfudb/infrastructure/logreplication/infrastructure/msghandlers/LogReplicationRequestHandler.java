package org.corfudb.infrastructure.logreplication.infrastructure.msghandlers;

import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * This is a method level annotation, used to identify the log replication request handlers in the LogReplicationServer.
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface LogReplicationRequestHandler {

    /**
     * Returns the request payload type
     * @return the type of log replication request payload
     */
    RequestPayloadMsg.PayloadCase  requestType();
}
