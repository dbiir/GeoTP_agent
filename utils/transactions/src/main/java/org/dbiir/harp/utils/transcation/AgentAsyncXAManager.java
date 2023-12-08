package org.dbiir.harp.utils.transcation;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class AgentAsyncXAManager {
    static AgentAsyncXAManager Instance = new AgentAsyncXAManager();
    public static int msgQueueLen = 4;

    public AgentAsyncXAManager() {
        for (int i = 0; i < msgQueueLen; i++) {
            messages.add(new LinkedList<>());
        }
        System.out.println(messages.size());
    }

    public static AgentAsyncXAManager getInstance() {
        return Instance;
    }

    private ChannelHandlerContext context;

    private final Collection<Thread> asyncThreads = new LinkedList<>();

    private final ConcurrentHashMap<CustomXID, XATransactionState> XAStates = new ConcurrentHashMap<>(2048, 0.75f, 256);

    private final List<List<AsyncMessageFromAgent>> messages = new ArrayList<>();

    private String algorithm = "";

    /**
     *
     * @param op - false represents remove, true represents add
     * @param message
     * @return message if messages not empty
     */
    synchronized public AsyncMessageFromAgent modifyMessages(boolean op, AsyncMessageFromAgent message, int id) {
        if (op) {
            messages.get(id % msgQueueLen).add(message);
        } else {
            if (messages.get(id).size() >= 1) {
                AsyncMessageFromAgent result = messages.get(id).get(0);
                messages.get(id).remove(0);
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
