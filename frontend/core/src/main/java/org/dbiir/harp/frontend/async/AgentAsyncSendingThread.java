package org.dbiir.harp.frontend.async;

import org.dbiir.harp.frontend.netty.AsyncMessageChannelInboundHandler;
import org.dbiir.harp.utils.transcation.AgentAsyncXAManager;
import org.dbiir.harp.utils.transcation.AsyncMessageFromAgent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class AgentAsyncSendingThread implements Runnable{
    @Override
    public void run() {
        while (true) {
            AsyncMessageFromAgent message = AgentAsyncXAManager.getInstance().modifyMessages(false, null);
            try {
                AsyncMessageChannelInboundHandler.sendMessage(serializeObject(message));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static byte[] serializeObject(Serializable obj) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(obj);
            objectOutputStream.close();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
