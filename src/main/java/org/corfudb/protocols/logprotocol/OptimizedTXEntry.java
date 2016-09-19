package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.serializer.ICorfuSerializable;
import org.corfudb.util.serializer.Serializers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Created by amytai on 9/16/16.
 */
@ToString
@NoArgsConstructor
@Slf4j
public class OptimizedTXEntry extends LogEntry {

    @Getter
    List<SMREntry> updates;

    public OptimizedTXEntry(List<SMREntry> updates) {
        this.type = LogEntryType.OPT_TX;
        this.updates = updates;
    }

    /**
     * This function provides the remaining buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);

        short numUpdates = b.readShort();
        updates = new ArrayList<>();
        for (short i = 0; i < numUpdates; i++) {
            updates.add(
                    (SMREntry) Serializers
                            .getSerializer(Serializers.SerializerType.CORFU)
                            .deserialize(b, rt));
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(updates.size());
        updates.stream()
                .forEach(x -> Serializers
                        .getSerializer(Serializers.SerializerType.CORFU)
                        .serialize(x, b));
    }
}
