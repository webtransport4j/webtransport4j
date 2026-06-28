package io.github.webtransport4j.server;

import org.jspecify.annotations.Nullable;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.NonNull;

public final class BusinessExecutorFactory {

    private static final Logger logger = LoggerFactory.getLogger(BusinessExecutorFactory.class);

    private BusinessExecutorFactory() {
    }

    public static @Nullable ExecutorService create() {
        DispatchExecutionMode mode = DispatchExecutionMode.valueOf(WebTransportConfig.get("webtransport4j.dispatch.execution.mode", "FIXED_THREAD_POOL"));
        switch(mode) {
            case NETTY_EVENT_LOOP:
                logger.info("Using Netty EventLoop for business callbacks");
                return null;
            case VIRTUAL_THREADS:
                return createVirtualThreadExecutor();
            case NETTY_EVENT_EXECUTOR_GROUP:
                return createDefaultEventExecutorGroup();
            case FIXED_THREAD_POOL:
            default:
                return createFixedThreadPool();
        }
    }

    private static @NonNull ExecutorService createDefaultEventExecutorGroup() {
        final int poolSize = WebTransportConfig.getInt("webtransport4j.netty.executor.group.size", Runtime.getRuntime().availableProcessors() * 2);
        logger.info("Using Netty DefaultEventExecutorGroup. poolSize={}", poolSize);
        return new io.netty.util.concurrent.DefaultEventExecutorGroup(poolSize, new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "wt-netty-executor-" + count.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    private static @NonNull ExecutorService createVirtualThreadExecutor() {
        try {
            Method method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            ExecutorService executor = (ExecutorService) method.invoke(null);
            logger.info("Using Java Virtual Threads for business callbacks");
            return executor;
        } catch (Throwable t) {
            logger.warn("Virtual threads unavailable. Falling back to fixed thread pool.");
            return createFixedThreadPool();
        }
    }

    private static @NonNull ExecutorService createFixedThreadPool() {
        final int poolSize = WebTransportConfig.getInt("webtransport4j.business.pool.size", Runtime.getRuntime().availableProcessors() * 2);
        final int rawQueueCapacity = WebTransportConfig.getInt("webtransport4j.business.queue.capacity", Integer.MAX_VALUE);
        final int queueCapacity = rawQueueCapacity <= 0 ? Integer.MAX_VALUE : rawQueueCapacity;
        final String queueType = WebTransportConfig.get("webtransport4j.business.queue.type", "RING_BUFFER");
        logger.info("Using fixed thread pool. poolSize={} , queueCapacity={}, queueType={}", poolSize, queueCapacity, queueType);
        BlockingQueue<Runnable> queue;
        if ("ARRAY".equalsIgnoreCase(queueType) || "RING_BUFFER".equalsIgnoreCase(queueType)) {
            if (queueCapacity == Integer.MAX_VALUE) {
                logger.warn("⚠️ ArrayBlockingQueue/RingBuffer cannot be used with unbounded queue capacity. Defaulting queue capacity to 10000.");
                queue = new ArrayBlockingQueue<>(10000);
            } else {
                queue = new ArrayBlockingQueue<>(queueCapacity);
            }
        } else {
            queue = queueCapacity == Integer.MAX_VALUE ? new LinkedBlockingQueue<>() : new LinkedBlockingQueue<>(queueCapacity);
        }
        return new ThreadPoolExecutor(poolSize, poolSize, 60L, TimeUnit.SECONDS, queue, new ThreadFactory() {

            private final AtomicInteger count = new AtomicInteger(1);

            @Override
            public @NonNull Thread newThread(@NonNull Runnable r) {
                Thread thread = new Thread(r, "wt-business-worker-" + count.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        }, createRejectedExecutionHandler(WebTransportConfig.get("webtransport4j.business.rejection.policy", "ABORT")));
    }

    private static @NonNull RejectedExecutionHandler createRejectedExecutionHandler(@NonNull String value) {
        switch(value.toUpperCase(Locale.ROOT)) {
            case "ABORT":
                return new ThreadPoolExecutor.AbortPolicy();
            case "CALLER_RUNS":
                return new ThreadPoolExecutor.CallerRunsPolicy();
            case "DISCARD":
                return new ThreadPoolExecutor.DiscardPolicy();
            case "DISCARD_OLDEST":
                return new ThreadPoolExecutor.DiscardOldestPolicy();
            default:
                try {
                    Class<?> clazz = Class.forName(value);
                    if (!RejectedExecutionHandler.class.isAssignableFrom(clazz)) {
                        throw new IllegalArgumentException(value + " does not implement " + RejectedExecutionHandler.class.getName());
                    }
                    return (RejectedExecutionHandler) clazz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unable to create RejectedExecutionHandler: " + value, e);
                }
        }
    }
}
