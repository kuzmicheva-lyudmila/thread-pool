package ru.kl;

import org.junit.jupiter.api.Test;
import ru.kl.threads.CustomFixedThreadPool;

import java.util.concurrent.RejectedExecutionException;

import static java.lang.Thread.sleep;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomFixedThreadPoolTest {

    private static final int THREAD_CAPACITY = 3;

    @Test
    void executedWithShutdownTest() throws InterruptedException {
        CustomFixedThreadPool threadPool = new CustomFixedThreadPool(THREAD_CAPACITY);
        prepareData(threadPool);

        sleep(1000);
        threadPool.shutdown();
        assertTrue(threadPool::isShutdown);
        await().until(threadPool::isTerminated);
    }

    @Test
    void executedWithShutdownNowTest() throws InterruptedException {
        CustomFixedThreadPool threadPool = new CustomFixedThreadPool(THREAD_CAPACITY);
        prepareData(threadPool);

        sleep(1000);
        threadPool.shutdownNow();

        assertTrue(threadPool::isShutdown);
        await().until(threadPool::isTerminated);
    }

    private void prepareData(CustomFixedThreadPool threadPool) {
        for (int i = 0; i < 10; i++) {
            threadPool.submit(
                    () -> {
                        sleep(100);
                        return "success";
                    }
            );
        }
    }

    @Test
    void addTaskWithShutdownTest() {
        CustomFixedThreadPool threadPool = new CustomFixedThreadPool(THREAD_CAPACITY);
        threadPool.shutdown();

        assertThrows(RejectedExecutionException.class, () -> threadPool.submit(() -> "success"));
    }

    @Test
    void incorrectTaskTest() {
        CustomFixedThreadPool threadPool = new CustomFixedThreadPool(THREAD_CAPACITY);
        assertThrows(NullPointerException.class, () -> threadPool.submit(null));
    }

    @Test
    void incorrectCapacityTest() {
        assertThrows(IllegalArgumentException.class, () -> new CustomFixedThreadPool(0));
    }
}
