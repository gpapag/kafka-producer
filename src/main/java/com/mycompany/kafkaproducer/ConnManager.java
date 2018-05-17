package com.mycompany.kafkaproducer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConnManager
{
    private static final Logger logger = LoggerFactory.getLogger(ConnManager.class);

    private final InetSocketAddress remoteAddress;
    private final KProducer kafkaProducer;

    private boolean connected;
    private EventLoopGroup workerGroup;

    public ConnManager(String remoteIpAddress, int remotePort, KProducer kafkaProducer)
    {
        requireNonNull(remoteIpAddress, "remoteIpAddress is null");
        requireNonNull(kafkaProducer, "kafkaProducer is null");
        checkArgument(remotePort > 0, "remotePort is negative");

        this.kafkaProducer = kafkaProducer;
        this.connected = false;
        this.workerGroup = null;
        this.remoteAddress = assignRemoteAddress(remoteIpAddress, remotePort);
        if (remoteAddress == null) {
            return;
        }

        initSocketConnection();
    }

    private InetSocketAddress assignRemoteAddress(String remoteIpAddress, int remotePort)
    {
        InetSocketAddress address = null;
        try {
            address = new InetSocketAddress(InetAddress.getByName(remoteIpAddress), remotePort);
        }
        catch (UnknownHostException e) {
            logger.error(e.getMessage());
        }
        return address;
    }

    private void initSocketConnection()
    {
        workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(remoteAddress)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                        {
                            ch.pipeline()
                              .addLast("MsgPackDecoder", new ConnFrameDecoderMsgPack())
                                 .addLast("ConnMessageHandler", new ConnMessageHandler(kafkaProducer));
                        }
                    });

            ChannelFuture channelFuture = b.connect().sync();
            if (channelFuture.isDone() && channelFuture.isSuccess()) {
                connected = true;
            }
            else {
                connected = false;
                channelFuture.channel().close();
            }
        }
        catch (Exception e) {
            logger.error("Connection to data source failed: " + e.getMessage());
            connected = false;
        }
    }

    public void shutDown()
    {
        workerGroup.shutdownGracefully();
    }

    public boolean isConnected()
    {
        return connected;
    }
}
