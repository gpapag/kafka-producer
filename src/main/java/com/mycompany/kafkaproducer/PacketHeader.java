package com.mycompany.kafkaproducer;

import io.netty.buffer.ByteBuf;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static java.util.Objects.requireNonNull;

public class PacketHeader
{
    private static final int HEADER_LENGTH = 36;

    private final Byte version;
    private final Byte flags;
    private final InetSocketAddress sourceAddress;
    private final Integer sequenceNumber;
    private final Integer totalNumberSegments;
    private final Long segmentTimestamp;
    private final Long tableTimestamp;
    private final Integer payloadSizeBytes;

    public PacketHeader(ByteBuf buffer) throws UnknownHostException
    {
        requireNonNull(buffer, "buffer is null");

        this.version = buffer.readByte();
        this.flags = buffer.readByte();

        byte[] portBytes = new byte[2];
        buffer.readBytes(portBytes);
        int port = 0;
        port += ((portBytes[0] & 0xFF) * (1 << 8));
        port += ((portBytes[1] & 0xFF));

        byte[] ipv4Bytes = new byte[4];
        buffer.readBytes(ipv4Bytes);

        this.sourceAddress = new InetSocketAddress(InetAddress.getByAddress(ipv4Bytes), port);

        this.sequenceNumber = buffer.readInt();
        this.totalNumberSegments = buffer.readInt();
        this.segmentTimestamp = buffer.readLong();
        this.tableTimestamp = buffer.readLong();
        this.payloadSizeBytes = buffer.readInt();

        //assert buffer.readerIndex() == HEADER_LENGTH;
    }

    public Byte getVersion()
    {
        return version;
    }

    public Byte getFlags()
    {
        return flags;
    }

    public InetSocketAddress getSourceAddress()
    {
        return sourceAddress;
    }

    public Integer getSequenceNumber()
    {
        return sequenceNumber;
    }

    public Integer getTotalNumberSegments()
    {
        return totalNumberSegments;
    }

    public Long getSegmentTimestamp()
    {
        return segmentTimestamp;
    }

    public Long getTableTimestamp()
    {
        return tableTimestamp;
    }

    public Integer getPayloadSizeBytes()
    {
        return payloadSizeBytes;
    }
}
