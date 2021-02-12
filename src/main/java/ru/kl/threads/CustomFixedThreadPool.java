package ru.kl.threads;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class CustomFixedThreadPool {

    private static final long THREAD_KEEP_ALIVE_TIME_MS = 10;

    private static final int MIN_ACTIVE_THREADS = 1;

    private final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();

    private final HashSet<Worker> workers = new HashSet<>();

    private final int threadCapacity;

    private final AtomicInteger activeThreadCount = new AtomicInteger(0);

    private final ReentrantLock mainLock = new ReentrantLock();

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public CustomFixedThreadPool(int threadCapacity) {
        if (threadCapacity <= 0) {
            throw new IllegalArgumentException("Count of threads is incorrect");
        }

        this.threadCapacity = threadCapacity;
    }

    public <T> Future<T> submit(Callable<T> task) {
        if (isShutdown.get()) {
            generateRejectedException();
        }
        if (Objects.isNull(task)) {
            throw new NullPointerException("The task cannot be null");
        }

        RunnableFuture<T> futureTask = new FutureTask<>(task);
        execute(futureTask);
        return futureTask;
    }

    public List<Runnable> shutdownNow() {
        List<Runnable> tasks = new ArrayList<>();
        ReentrantLock lock = this.mainLock;
        lock.lock();
        try {
            isShutdown.set(true);
            interruptWorkers();
            workQueue.drainTo(tasks);
        } finally {
            lock.unlock();
        }
        return tasks;
    }

    public void shutdown() {
        isShutdown.set(true);
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public boolean isTerminated() {
        if (!isShutdown.get()) {
            return false;
        }

        boolean result = true;
        ReentrantLock lock = this.mainLock;
        lock.lock();
        try {
            for (Worker worker: workers) {
                Thread thread = worker.thread;
                if (Objects.nonNull(worker.thread) && thread.getState() != Thread.State.TERMINATED) {
                    result = false;
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        return result;
    }

    private void execute(Runnable command) {
        if (!isShutdown.get() && workQueue.offer(command)) {
            if (activeThreadCount.get() < threadCapacity) {
                addWorker();
            }
        } else {
            generateRejectedException();
        }
    }

    private void generateRejectedException() {
        throw new RejectedExecutionException("The task cannot be executed");
    }

    private void addWorker() {
        boolean workerStarted = false;
        boolean workerAdded = false;
        Worker worker = null;
        try {
            worker = new Worker();
            Thread t = worker.thread;
            if (Objects.nonNull(t)) {
                ReentrantLock lock = this.mainLock;
                lock.lock();
                try {
                    if (activeThreadCount.get() < threadCapacity && !workQueue.isEmpty()) {
                        do {
                        } while (!compareAndIncrementWorkerCount(activeThreadCount.get()));
                        workers.add(worker);
                        workerAdded = true;
                        log.info(
                                "Worker added: thread = {}, activeThreadCount = {}",
                                Thread.currentThread().getName(),
                                activeThreadCount.get()
                        );
                    }
                } finally {
                    lock.unlock();
                }
                if (workerAdded) {
                    t.start();
                    workerStarted = true;
                    log.info("Worker started: thread = {}", t.getName());
                }
            }
        } finally {
            if (!workerStarted) {
                removeWorker(worker, true);
            }
        }
    }

    final void runWorker(Worker worker) {
        Thread thread = Thread.currentThread();
        log.info("Worker run: thread = {}", thread.getName());
        Runnable task;
        for (; ; ) {
            task = getTask();
            if (Objects.isNull(task)) {
                if (isShutdown.get() || removeWorker(worker, false)) {
                    return;
                } else {
                    continue;
                }
            }
            try {
                task.run();
                log.info("Task complete: thread = {}", thread.getName());
            } catch (RuntimeException | Error e) {
                log.error("Task run with error: thread = {}", thread.getName(), e);
                throw e;
            } catch (Throwable e) {
                log.error("Task run with error: thread = {}", thread.getName(), e);
                throw new Error(e);
            }
        }
    }

    private Runnable getTask() {
        if (workQueue.isEmpty()) {
            return null;
        }

        try {
            return workQueue.poll(THREAD_KEEP_ALIVE_TIME_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException retry) {
            return null;
        }
    }

    private boolean removeWorker(Worker worker, boolean isFailed) {
        boolean result = false;
        ReentrantLock lock = this.mainLock;
        lock.lock();
        try {
            if ((activeThreadCount.get() > MIN_ACTIVE_THREADS || isFailed) && Objects.nonNull(worker)) {
                boolean isRemoved = workers.remove(worker);
                if (isRemoved) {
                    do {
                    } while (!compareAndDecrementWorkerCount(activeThreadCount.get()));

                    result = true;
                    log.info(
                            "Worker removed: thread = {}, activeThreadCount = {}, isFailed = {}",
                            Thread.currentThread().getName(),
                            activeThreadCount.get(),
                            isFailed
                    );
                }
            }
        } finally {
            lock.unlock();
        }

        return result;
    }

    private boolean compareAndDecrementWorkerCount(int expect) {
        return activeThreadCount.compareAndSet(expect, expect - 1);
    }

    private boolean compareAndIncrementWorkerCount(int expect) {
        return activeThreadCount.compareAndSet(expect, expect + 1);
    }

    private void interruptWorkers() {
        ReentrantLock lock = this.mainLock;
        lock.lock();
        try {
            for (Worker w : workers)
                w.interruptIfStarted();
        } finally {
            lock.unlock();
        }
    }

    private final class Worker implements Runnable {

        final Thread thread;

        Worker() {
            this.thread = new Thread(this);
        }

        @Override
        public void run() {
            runWorker(this);
        }

        void interruptIfStarted() {
            Thread t = thread;
            if (Objects.nonNull(t) && !t.isInterrupted()) {
                try {
                    t.interrupt();
                    log.info("Thread interrupted: thread = {}", t.getName());
                } catch (SecurityException ignore) {
                }
            }
        }
    }
}
