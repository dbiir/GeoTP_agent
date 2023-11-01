package org.dbiir.harp.frontend.async;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.dbiir.harp.utils.transcation.AsyncMessageFromAgent;

import java.nio.charset.StandardCharsets;

public class AsyncMessageEncoder extends MessageToByteEncoder<AsyncMessageFromAgent> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, AsyncMessageFromAgent message, ByteBuf out) {
        byte[] contentBytes = message.toString().getBytes(StandardCharsets.UTF_8);

        out.writeInt(contentBytes.length); // write in message length
        out.writeBytes(contentBytes); // write in message content
    }
}
