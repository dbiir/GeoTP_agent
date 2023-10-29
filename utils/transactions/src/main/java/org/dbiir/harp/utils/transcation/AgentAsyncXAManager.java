package org.dbiir.harp.utils.transcation;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@Getter
@RequiredArgsConstructor
public class AgentAsyncXAManager {
    static AgentAsyncXAManager Instance = new AgentAsyncXAManager();

    public static AgentAsyncXAManager getInstance() {
        return Instance;
    }

    private ChannelHandlerContext context;

    private final Collection<Thread> asyncThreads = new LinkedList<>();

    private final HashMap<CustomXID, XATransactionState> XAStates = new HashMap<>();

    private final List<AsyncMessageFromAgent> messages = new LinkedList<>();

    private String algorithm = "";

    /**
     *
     * @param op - false represents remove, true represents add
     * @param message
     * @return message if messages not empty
     */
    synchronized public AsyncMessageFromAgent modifyMessages(boolean op, AsyncMessageFromAgent message) {
        if (op) {
            messages.add(message);
        } else {
            if (messages.size() >= 1) {
                AsyncMessageFromAgent result = messages.get(0);
                messages.remove(0);
                return result;
            }
        }
        return null;
    }

    public void setContext(ChannelHandlerContext context) {
        this.context = context;
    }

    public void setAlgorithm(String alg) {
        this.algorithm = alg.toLowerCase();
    }

    public boolean asyncPreparation() {
        return algorithm.equals("aharp") || algorithm.equals("aharp_lp") || algorithm.equals("aharp_pa") ||
                algorithm.equals("aharp_lppa") || algorithm.equals("a");
    }

    synchronized public void addAsyncThread(Thread thread) {
        asyncThreads.add(thread);
        thread.start();
    }
}
