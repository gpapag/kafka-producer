package com.mycompany.kafkaproducer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ConnMessageHandler
        extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(ConnMessageHandler.class);

    private final KProducer kafkaProducer;

    private Integer currentPartition;

    public ConnMessageHandler(KProducer kafkaProducer)
    {
        super();
        this.kafkaProducer = requireNonNull(kafkaProducer, "kafkaProducer is null");
        this.currentPartition = 0;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        ByteBuf buffer = (ByteBuf) msg;

        logger.debug("Received Message[{}]", buffer.toString(CharsetUtil.UTF_8));

        if (!processMsgPack(buffer)) {
            logger.error("Unable to send MsgPack message");
        }
        buffer.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        cause.printStackTrace();
    }

    private boolean processMsgPack(ByteBuf buffer)
    {
        ConnMessageDecoderMsgPack connMessageDecoderMsgPack = new ConnMessageDecoderMsgPack(buffer);
        if (!connMessageDecoderMsgPack.isDecodingComplete()) {
            logger.error(connMessageDecoderMsgPack.getDecodingError());
            return false;
        }

        printMsgPacket(connMessageDecoderMsgPack);

        Integer partition = currentPartition++ % kafkaProducer.getNumPartitions();
        String key = connMessageDecoderMsgPack.getTableName().orElse("no-table");
        byte[] value = connMessageDecoderMsgPack.getPayload().orElse(new byte[]{});

        return kafkaProducer.produceBytes(partition, key, value);
    }

    private void printMsgPacket(ConnMessageDecoderMsgPack connMessageDecoderMsgPack)
    {
        PacketHeader packetHeader = connMessageDecoderMsgPack.getHeader().get();
        StringBuilder sb = new StringBuilder();
        sb.append("Packet Header:\n");
        sb.append(String.format("\tversion: %d\n", packetHeader.getVersion()));
        sb.append(String.format("\tflags: %x\n", packetHeader.getFlags()));
        sb.append(String.format("\tsource address: %s\n", packetHeader.getSourceAddress().toString()));
        sb.append(String.format("\tseq num: %d\n", packetHeader.getSequenceNumber()));
        sb.append(String.format("\ttotal: %d\n", packetHeader.getTotalNumberSegments()));
        sb.append(String.format("\tseq ts: %d\n", packetHeader.getSegmentTimestamp()));
        sb.append(String.format("\ttable ts: %d\n", packetHeader.getTableTimestamp()));
        sb.append(String.format("\tpayload size: %d\n", packetHeader.getPayloadSizeBytes()));
        sb.append(String.format("\nTable Name:\n"));
        sb.append(String.format("\t%s\n", connMessageDecoderMsgPack.getTableName().get()));
        sb.append(String.format("\nPayload:\n"));

        MessageUnpacker messageUnpacker = MessagePack.newDefaultUnpacker(connMessageDecoderMsgPack.getPayload().get());
        try {
            while (messageUnpacker.hasNext()) {
                Value v = messageUnpacker.unpackValue();

                switch (v.getValueType()) {
                    case STRING:
                        sb.append(String.format("(string) %s\n", v.asStringValue().asString()));
                        break;
                    case ARRAY:
                        ArrayValue arrayValue = v.asArrayValue();
                        for (Value arrayElement : arrayValue) {
                            if (arrayElement.isArrayValue()) {
                                for (Value e : (ArrayValue) arrayElement) {
                                    switch (e.getValueType()) {
                                        case STRING:
                                            sb.append(String.format("\t(string) %s ", e.asStringValue().asString()));
                                            break;
                                        case INTEGER:
                                            sb.append(String.format("\t(integer) %d ", e.asIntegerValue().asLong()));
                                            break;
                                        default:
                                            sb.append(String.format("\t(-) %s ", e.asStringValue().asString()));
                                            break;
                                    }
                                }
                            }
                            else {
                                sb.append(String.format("\t(non-array-element) %s ", arrayElement));
                            }
                            sb.append("\n");
                        }
                        break;
                    default:
                        logger.error("Unknown type in payload");
                        break;
                }
            }
        }
        catch (IOException e) {
            sb.append(e.getMessage());
        }

        logger.debug(sb.toString());
    }
}
