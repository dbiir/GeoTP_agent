package org.dbiir.harp.utils.transcation;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class AgentAsyncThreadCollector implements Runnable{
    @Override
    public void run() {
        while (true) {
            Collection<Thread> threadList = AgentAsyncXAManager.getInstance().getAsyncThreads();

            Iterator<Thread> iterator = threadList.iterator();

            while (iterator.hasNext()) {
                Thread thread = iterator.next();
                if (!thread.isAlive()) {
                    // thread is over
                    iterator.remove(); // save remove
                }
            }
            // TODO: garbage collect xid hashmap

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
