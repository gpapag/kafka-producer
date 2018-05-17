package com.mycompany.kafkaproducer;

import io.netty.buffer.ByteBuf;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConnMessageDecoderMsgPack
{
    private PacketHeader packetHeader;
    private String tableName;
    private byte[] payload;

    private boolean decodingComplete;
    private String decodingError;

    public ConnMessageDecoderMsgPack(ByteBuf buffer)
    {
        requireNonNull(buffer, "buffer is null");

        this.decodingComplete = false;
        this.decodingError = "";

        try {
            this.packetHeader = new PacketHeader(buffer);
        }
        catch (UnknownHostException e) {
            decodingError = e.getMessage();
            return;
        }

        this.payload = new byte[packetHeader.getPayloadSizeBytes()];
        buffer.readBytes(payload);

        MessageUnpacker messageUnpacker = MessagePack.newDefaultUnpacker(payload);
        try {
            if (messageUnpacker.hasNext()) {
                Value v = messageUnpacker.unpackValue();

                if (v.getValueType().isStringType()) {
                    this.tableName = v.asStringValue().asString();
                }
                else {
                    decodingError = String.format("Payload does not start with tablename string");
                    return;
                }
            }
            messageUnpacker.close();
        }
        catch (IOException e) {
            decodingError = e.getMessage();
            return;
        }

        this.decodingComplete = true;
    }

    public boolean isDecodingComplete()
    {
        return decodingComplete;
    }

    public String getDecodingError()
    {
        return decodingError;
    }

    public Optional<PacketHeader> getHeader()
    {
        if (!decodingComplete) {
            return Optional.empty();
        }
        return Optional.of(packetHeader);
    }

    public Optional<String> getTableName()
    {
        if (!decodingComplete) {
            return Optional.empty();
        }
        return Optional.of(tableName);
    }

    public Optional<byte[]> getPayload()
    {
        if (!decodingComplete) {
            return Optional.empty();
        }
        return Optional.of(payload);
    }
}
