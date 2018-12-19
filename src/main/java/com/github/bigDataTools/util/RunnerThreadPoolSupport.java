package com.github.bigDataTools.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池实现
 */
public class RunnerThreadPoolSupport {

    private static ExecutorService executorService;

    public static ExecutorService getExecutorService() {
        if (executorService == null) {
            executorService = new ThreadPoolExecutor(
                    100,500,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(1000),
                    new NamedThreadFactory("ltsTasker"),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
        }
        return executorService;
    }

    public static void setExecutorService(ExecutorService executorService) {
        RunnerThreadPoolSupport.executorService = executorService;
    }

    public static void destory() {
        executorService.shutdown();
    }
}
