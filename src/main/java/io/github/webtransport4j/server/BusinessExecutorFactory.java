package io.github.webtransport4j.server;

import java.lang.reflect.Method;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

public final class BusinessExecutorFactory {

    private static final Logger logger =
        Logger.getLogger(BusinessExecutorFactory.class);

    private BusinessExecutorFactory() {}

    public static ExecutorService create() {

        DispatchExecutionMode mode =
            DispatchExecutionMode.valueOf(
                WebTransportConfig.get(
                    "webtransport4j.dispatch.execution.mode",
                    "FIXED_THREAD_POOL"));

        switch (mode) {

            case NETTY_EVENT_LOOP:
                logger.info(
                    "Using Netty EventLoop for business callbacks");
                return null;

            case VIRTUAL_THREADS:
                return createVirtualThreadExecutor();

            case FIXED_THREAD_POOL:
            default:
                return createFixedThreadPool();
        }
    }

    private static ExecutorService createVirtualThreadExecutor() {

        try {

            Method method =
                Executors.class.getMethod(
                    "newVirtualThreadPerTaskExecutor");

            ExecutorService executor =
                (ExecutorService) method.invoke(null);

            logger.info(
                "Using Java Virtual Threads for business callbacks");

            return executor;

        } catch (Throwable t) {

            logger.warn(
                "Virtual threads unavailable. "
                    + "Falling back to fixed thread pool.");

            return createFixedThreadPool();
        }
    }

    private static ExecutorService createFixedThreadPool() {

        final int poolSize =
            WebTransportConfig.getInt(
                "webtransport4j.business.pool.size",
                Runtime.getRuntime().availableProcessors() * 2);

        final int rawQueueCapacity =
            WebTransportConfig.getInt(
                "webtransport4j.business.queue.capacity",
                Integer.MAX_VALUE);

        final int queueCapacity = rawQueueCapacity <= 0 ? Integer.MAX_VALUE : rawQueueCapacity;

        final String queueType =
            WebTransportConfig.get(
                "webtransport4j.business.queue.type",
                "RING_BUFFER");

        logger.info(
            "Using fixed thread pool. poolSize="
                + poolSize
                + ", queueCapacity="
                + queueCapacity
                + ", queueType="
                + queueType);

        BlockingQueue<Runnable> queue;
        if ("ARRAY".equalsIgnoreCase(queueType) || "RING_BUFFER".equalsIgnoreCase(queueType)) {
            if (queueCapacity == Integer.MAX_VALUE) {
                logger.warn(
                    "⚠️ ArrayBlockingQueue/RingBuffer cannot be used with unbounded queue capacity. "
                        + "Defaulting queue capacity to 10000.");
                queue = new ArrayBlockingQueue<Runnable>(10000);
            } else {
                queue = new ArrayBlockingQueue<Runnable>(queueCapacity);
            }
        } else {
            queue = queueCapacity == Integer.MAX_VALUE
                ? new LinkedBlockingQueue<Runnable>()
                : new LinkedBlockingQueue<Runnable>(queueCapacity);
        }

        return new ThreadPoolExecutor(
            poolSize,
            poolSize,
            60L,
            TimeUnit.SECONDS,
            queue,
            new ThreadFactory() {

                private final AtomicInteger count =
                    new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {

                    Thread thread =
                        new Thread(
                            r,
                            "wt-business-worker-"
                                + count.getAndIncrement());

                    thread.setDaemon(true);

                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
