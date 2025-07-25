/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery.EMPTY_PARAMS;

/**
 * Request for DML operation on remote node.
 */
public class GridH2DmlRequest implements Message, GridCacheQueryMarshallable {
    /** Request id. */
    @GridToStringInclude
    private long reqId;

    /** Cache identifiers. */
    @GridToStringInclude
    @GridDirectCollection(Integer.class)
    private List<Integer> caches;

    /** Topology version. */
    @GridToStringInclude
    private AffinityTopologyVersion topVer;

    /** Query partitions. */
    @GridToStringInclude
    private int[] qryParts;

    /** Page size. */
    private int pageSize;

    /** Query. */
    @GridToStringInclude
    private String qry;

    /** Flags. */
    private byte flags;

    /** Timeout. */
    private int timeout;

    /** Query parameters. */
    @GridToStringInclude(sensitive = true)
    @GridDirectTransient
    private Object[] params;

    /** Query parameters as bytes. */
    private byte[] paramsBytes;

    /** Schema name. */
    @GridToStringInclude
    private String schemaName;

    /** Explicit timeout flag. */
    @GridToStringInclude
    private boolean explicitTimeout;

    /**
     * Empty constructor.
     */
    public GridH2DmlRequest() {
        // No-op.
    }

    /**
     * @param req Request.
     */
    public GridH2DmlRequest(GridH2DmlRequest req) {
        reqId = req.reqId;
        caches = req.caches;
        topVer = req.topVer;
        qryParts = req.qryParts;
        pageSize = req.pageSize;
        qry = req.qry;
        flags = req.flags;
        timeout = req.timeout;
        params = req.params;
        paramsBytes = req.paramsBytes;
        schemaName = req.schemaName;
        explicitTimeout = req.explicitTimeout;
    }

    /**
     * @return Parameters.
     */
    public Object[] parameters() {
        return params;
    }

    /**
     * @param params Parameters.
     * @return {@code this}.
     */
    public GridH2DmlRequest parameters(Object[] params) {
        if (params == null)
            params = EMPTY_PARAMS;

        this.params = params;

        return this;
    }

    /**
     * @param reqId Request ID.
     * @return {@code this}.
     */
    public GridH2DmlRequest requestId(long reqId) {
        this.reqId = reqId;

        return this;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param caches Caches.
     * @return {@code this}.
     */
    public GridH2DmlRequest caches(List<Integer> caches) {
        this.caches = caches;

        return this;
    }

    /**
     * @return Caches.
     */
    public List<Integer> caches() {
        return caches;
    }

    /**
     * @param topVer Topology version.
     * @return {@code this}.
     */
    public GridH2DmlRequest topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;

        return this;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Query partitions.
     */
    public int[] queryPartitions() {
        return qryParts;
    }

    /**
     * @param qryParts Query partitions.
     * @return {@code this}.
     */
    public GridH2DmlRequest queryPartitions(int[] qryParts) {
        this.qryParts = qryParts;

        return this;
    }

    /**
     * @param pageSize Page size.
     * @return {@code this}.
     */
    public GridH2DmlRequest pageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param qry SQL Query.
     * @return {@code this}.
     */
    public GridH2DmlRequest query(String qry) {
        this.qry = qry;

        return this;
    }

    /**
     * @return SQL Query.
     */
    public String query() {
        return qry;
    }

    /**
     * @param flags Flags.
     * @return {@code this}.
     */
    public GridH2DmlRequest flags(int flags) {
        assert flags >= 0 && flags <= 255 : flags;

        this.flags = (byte)flags;

        return this;
    }

    /**
     * @param flags Flags to check.
     * @return {@code true} If all the requested flags are set to {@code true}.
     */
    public boolean isFlagSet(int flags) {
        return (this.flags & flags) == flags;
    }

    /**
     * Checks if data page scan enabled.
     *
     * @return {@code true} If data page scan enabled, {@code false} if not, and {@code null} if not set.
     */
    public Boolean isDataPageScanEnabled() {
        return GridH2QueryRequest.isDataPageScanEnabled(flags);
    }

    /**
     * @return Timeout.
     */
    public int timeout() {
        return timeout;
    }

    /**
     * @param timeout New timeout.
     * @return {@code this}.
     */
    public GridH2DmlRequest timeout(int timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return {@code this}.
     */
    public GridH2DmlRequest schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Explicit timeout flag.
     */
    public boolean explicitTimeout() {
        return explicitTimeout;
    }

    /**
     * @param explicitTimeout Explicit timeout flag.
     * @return {@code this}.
     */
    public GridH2DmlRequest explicitTimeout(boolean explicitTimeout) {
        this.explicitTimeout = explicitTimeout;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void marshall(BinaryMarshaller m) {
        if (paramsBytes != null)
            return;

        assert params != null;

        try {
            paramsBytes = U.marshal(m, params);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override public void unmarshall(GridKernalContext ctx) {
        if (params != null)
            return;

        assert paramsBytes != null;

        final ClassLoader ldr = U.resolveClassLoader(ctx.config());

        // To avoid deserializing of enum types.
        params = BinaryUtils.rawArrayFromBinary(ctx.marshaller().binaryMarshaller().unmarshal(paramsBytes, ldr));
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(caches, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean(explicitTimeout))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByte(flags))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeInt(pageSize))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray(paramsBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeString(qry))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeIntArray(qryParts))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong(reqId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeString(schemaName))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeInt(timeout))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage(topVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                caches = reader.readCollection(MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                explicitTimeout = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                flags = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                pageSize = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                paramsBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                qry = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                qryParts = reader.readIntArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                reqId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                schemaName = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                timeout = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                topVer = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -55;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2DmlRequest.class, this);
    }
}
