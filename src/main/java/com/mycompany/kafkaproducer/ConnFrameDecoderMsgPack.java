package com.mycompany.kafkaproducer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class ConnFrameDecoderMsgPack
        extends ByteToMessageDecoder
{
    private static final int HEADER_LENGTH = 36;

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    {
        if (in.readableBytes() < HEADER_LENGTH) {
            return;
        }

        byte[] headerBytes = new byte[HEADER_LENGTH];
        in.getBytes(in.readerIndex(), headerBytes, 0, HEADER_LENGTH);

        int payloadLength = getPayloadLength(headerBytes);
        if (in.readableBytes() < HEADER_LENGTH + payloadLength) {
            return;
        }

        in.skipBytes(HEADER_LENGTH);

        byte[] payload = new byte[payloadLength];
        in.readBytes(payload, 0, payloadLength);

        ByteBuf packet = ctx.alloc().heapBuffer(HEADER_LENGTH + payloadLength);
        packet.writeBytes(headerBytes);
        packet.writeBytes(payload);

        //assert packet.writerIndex() == (HEADER_LENGTH + payloadLength);

        out.add(packet);

        //assert in.refCnt() == 1;
        //in.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        cause.printStackTrace();
        //ctx.close();
    }

    private int getPayloadLength(byte[] header)
    {
        int result = 0;

        result += ((header[32] & 0xFF) * (1 << 24));
        result += ((header[33] & 0xFF) * (1 << 16));
        result += ((header[34] & 0xFF) * (1 << 8));
        result += ((header[35] & 0xFF));

        if (result < 0) {
            throw new IllegalStateException("Failed to retrieve payload length");
        }

        return result;
    }
}
