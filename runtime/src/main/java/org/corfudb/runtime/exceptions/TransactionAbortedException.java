package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;

/**
 * Created by mwei on 1/11/16.
 */
public class TransactionAbortedException extends RuntimeException {

    @Getter
    AbortCause abortCause;

    @Getter
    TxResolutionInfo txResolutionInfo;

    @Getter
    Long conflictKey;

    @Getter
    Throwable cause;

    /**
     * Constructor.
     * @param txResolutionInfo transaction information
     * @param conflictKey conflict key
     * @param abortCause cause
     */
    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            Long conflictKey, AbortCause abortCause) {
        this(txResolutionInfo, conflictKey, abortCause, null);
    }

    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            Long conflictKey, AbortCause abortCause, Throwable cause) {
        super("TX ABORT "
                + " | Snapshot Time = " + txResolutionInfo.getSnapshotTimestamp()
                + " | Transaction ID = " + txResolutionInfo.getTXid()
                + " | Conflict Key = " + conflictKey
                + " | Cause = " + abortCause);
        this.txResolutionInfo = txResolutionInfo;
        this.conflictKey = conflictKey;
        this.abortCause = abortCause;
        this.cause = cause;

    }

}
