package com.example.gsb.semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class FairSemaphoreTest {

    private static void awaitCondition(Check check, String description) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!check.ok()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + description);
            }
            Thread.sleep(2);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    // ------------------------------------------------------------------
    // 1) 基本获取与释放
    // ------------------------------------------------------------------

    @Test
    void acquireAndReleaseUpdatesAvailablePermits() {
        FairSemaphore semaphore = new FairSemaphore(2);

        semaphore.acquire();
        assertThat(semaphore.getAvailablePermits()).isEqualTo(1);
        semaphore.acquire();
        assertThat(semaphore.getAvailablePermits()).isEqualTo(0);
        assertThat(semaphore.tryAcquire()).isFalse();

        semaphore.release();
        assertThat(semaphore.getAvailablePermits()).isEqualTo(1);
        semaphore.release();
        assertThat(semaphore.getAvailablePermits()).isEqualTo(2);
        assertThat(semaphore.getTotalAcquisitions()).isEqualTo(2);
    }

    @Test
    void releaseWithoutAcquireThrows() {
        FairSemaphore semaphore = new FairSemaphore(1);
        assertThatThrownBy(semaphore::release).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void invalidPermitCountsAreRejected() {
        assertThatThrownBy(() -> new FairSemaphore(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FairSemaphore(1).resize(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------
    // 2) 公平性：FIFO，无长期插队
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void waitersAreGrantedPermitsInFifoOrder() throws Exception {
        int waiterCount = 6;
        for (int round = 0; round < 3; round++) {
            FairSemaphore semaphore = new FairSemaphore(1);
            semaphore.acquire(); // 主线程占住唯一许可，迫使后续线程排队

            List<Integer> grantOrder = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch enteredCritical = new CountDownLatch(waiterCount);
            List<Thread> threads = new ArrayList<>();
            for (int index = 0; index < waiterCount; index++) {
                final int id = index;
                Thread thread = new Thread(() -> {
                    semaphore.acquire();
                    grantOrder.add(id);
                    enteredCritical.countDown();
                    try {
                        Thread.sleep(15); // 持有一段时间，放大插队机会
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        semaphore.release();
                    }
                });
                thread.start();
                threads.add(thread);
                // 确保该线程确实已入队后再启动下一个，构造确定的 FIFO 顺序
                awaitCondition(() -> semaphore.getWaitingThreadCount() == id + 1,
                        "waiter " + id + " enqueued");
            }

            semaphore.release(); // 放行，许可应严格按入队顺序传递
            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(grantOrder)
                    .as("round %d grant order", round)
                    .containsExactly(0, 1, 2, 3, 4, 5);
            assertThat(semaphore.getWaitingThreadCount()).isZero();
            assertThat(semaphore.getAvailablePermits()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(30)
    void lateArriverCannotBargeAheadOfQueuedWaiter() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(1);
        semaphore.acquire();

        CountDownLatch firstMayRelease = new CountDownLatch(1);
        AtomicBoolean firstAcquired = new AtomicBoolean(false);
        Thread first = new Thread(() -> {
            semaphore.acquire();
            firstAcquired.set(true);
            try {
                firstMayRelease.await(); // 持许可直到主线程完成断言
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                semaphore.release();
            }
        });
        first.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "first waiter enqueued");

        // 后来者此刻调用 acquire，即使有线程即将 release，也必须排在 first 之后
        AtomicBoolean lateAcquired = new AtomicBoolean(false);
        Thread late = new Thread(() -> {
            semaphore.acquire();
            lateAcquired.set(true);
            semaphore.release();
        });
        late.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 2, "late waiter enqueued");

        semaphore.release(); // 只能授予队首 first，first 持许可直到断言后才归还
        awaitCondition(firstAcquired::get, "first waiter granted before late arriver");
        assertThat(semaphore.getWaitingThreadCount()).isEqualTo(1); // late 仍在排队
        assertThat(lateAcquired).isFalse();

        firstMayRelease.countDown(); // first 归还后许可才轮到 late
        first.join();
        late.join();
        assertThat(lateAcquired).isTrue();
        assertThat(semaphore.getAvailablePermits()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // 3) 超时获取：移除等待记录且不消耗许可
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void timedOutWaiterIsRemovedAndDoesNotConsumePermit() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(1);
        semaphore.acquire();

        AtomicReference<Boolean> result = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                result.set(semaphore.tryAcquire(150, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "waiter enqueued");
        waiter.join();

        assertThat(result.get()).isFalse();
        assertThat(semaphore.getWaitingThreadCount()).isZero(); // 等待记录已移除
        assertThat(semaphore.getTimeoutCount()).isEqualTo(1);
        assertThat(semaphore.getAvailablePermits()).isZero(); // 许可仍被主线程持有

        // 归还后许可必须完整可用，未被超时线程消耗
        semaphore.release();
        assertThat(semaphore.tryAcquire()).isTrue();
        assertThat(semaphore.getAvailablePermits()).isZero();
        semaphore.release();
    }

    @Test
    @Timeout(30)
    void timedAcquireSucceedsWhenPermitArrivesBeforeDeadline() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(1);
        semaphore.acquire();

        AtomicReference<Boolean> result = new AtomicReference<>(false);
        Thread waiter = new Thread(() -> {
            try {
                result.set(semaphore.tryAcquire(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "waiter enqueued");

        Thread.sleep(100);
        semaphore.release();
        waiter.join();

        assertThat(result.get()).isTrue();
        assertThat(semaphore.getTimeoutCount()).isZero();
        semaphore.release();
    }

    // ------------------------------------------------------------------
    // 4) 动态调整总许可数
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void resizeUpImmediatelyUnblocksWaiters() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(1);
        semaphore.acquire();

        AtomicBoolean acquired = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            semaphore.acquire();
            acquired.set(true);
        });
        waiter.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "waiter enqueued");

        semaphore.resize(2); // 扩容：无需等待归还，立即向队首发许可
        awaitCondition(acquired::get, "waiter granted after resize up");
        waiter.join();

        assertThat(semaphore.getTotalPermits()).isEqualTo(2);
        assertThat(semaphore.getAvailablePermits()).isZero();
        semaphore.release();
        semaphore.release();
        assertThat(semaphore.getAvailablePermits()).isEqualTo(2);
    }

    @Test
    @Timeout(30)
    void resizeDownBelowIssuedTakesEffectLazily() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(3);
        semaphore.acquire();
        semaphore.acquire();
        semaphore.acquire();
        assertThat(semaphore.getAvailablePermits()).isZero();

        // 缩容到 1，低于已发放的 3：已发放许可继续有效，期间不再发放新许可
        semaphore.resize(1);
        assertThat(semaphore.getTotalPermits()).isEqualTo(1);
        assertThat(semaphore.getAvailablePermits()).isZero();

        AtomicBoolean blockedAcquired = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            semaphore.acquire();
            blockedAcquired.set(true);
        });
        waiter.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "waiter enqueued");

        semaphore.release(); // issued: 3 -> 2，仍高于新上限 1
        semaphore.release(); // issued: 2 -> 1，等于上限，仍不能发放
        Thread.sleep(150);
        assertThat(blockedAcquired).as("no new grant while issued >= reduced total").isFalse();
        assertThat(semaphore.getAvailablePermits()).isZero();

        semaphore.release(); // issued: 1 -> 0，低于上限，队首获得许可
        awaitCondition(blockedAcquired::get, "waiter granted after permits returned below limit");
        waiter.join();
        assertThat(semaphore.getAvailablePermits()).isZero();
        semaphore.release();
        assertThat(semaphore.getAvailablePermits()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // 5) 可中断获取
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void interruptedWaiterIsRemovedAndDoesNotConsumePermit() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(1);
        semaphore.acquire();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                semaphore.acquireInterruptibly();
            } catch (InterruptedException e) {
                failure.set(e);
            }
        });
        waiter.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "waiter enqueued");

        waiter.interrupt();
        waiter.join();

        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(semaphore.getWaitingThreadCount()).isZero(); // 无残留等待记录
        assertThat(semaphore.getAvailablePermits()).isZero();   // 许可未被消耗

        semaphore.release();
        assertThat(semaphore.tryAcquire()).isTrue(); // 许可仍完整可用
        semaphore.release();
    }

    @Test
    @Timeout(30)
    void acquireInterruptiblyThrowsImmediatelyWhenAlreadyInterrupted() {
        FairSemaphore semaphore = new FairSemaphore(1);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(semaphore::acquireInterruptibly)
                    .isInstanceOf(InterruptedException.class);
            assertThat(semaphore.getAvailablePermits()).isEqualTo(1);
        } finally {
            Thread.interrupted(); // 清除中断标记，避免影响后续测试
        }
    }

    @Test
    @Timeout(30)
    void uninterruptibleAcquireSurvivesInterruptionAndRestoresFlag() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(1);
        semaphore.acquire();

        AtomicBoolean acquired = new AtomicBoolean(false);
        AtomicBoolean interruptFlagSeen = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            semaphore.acquire();
            acquired.set(true);
            interruptFlagSeen.set(Thread.currentThread().isInterrupted());
        });
        waiter.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "waiter enqueued");

        waiter.interrupt(); // acquire() 不响应中断，应继续排队
        Thread.sleep(100);
        assertThat(acquired).isFalse();

        semaphore.release();
        waiter.join();
        assertThat(acquired).isTrue();
        assertThat(interruptFlagSeen).as("interrupt status restored after acquisition").isTrue();
        semaphore.release();
    }

    // ------------------------------------------------------------------
    // 6) 并发守恒：已发放数永不超过总许可数
    // ------------------------------------------------------------------

    @Test
    @Timeout(120)
    void permitCountIsConservedUnderConcurrency() throws Exception {
        int totalPermits = 5;
        int threadCount = 16;
        int iterationsPerThread = 300;
        FairSemaphore semaphore = new FairSemaphore(totalPermits);

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        AtomicInteger acquireCount = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            futures.add(pool.submit(() -> {
                for (int i = 0; i < iterationsPerThread; i++) {
                    semaphore.acquire();
                    int now = inFlight.incrementAndGet();
                    maxObserved.accumulateAndGet(now, Math::max);
                    acquireCount.incrementAndGet();
                    // 临界区内做一点随机工作，增加交错
                    if (ThreadLocalRandom.current().nextInt(10) == 0) {
                        Thread.yield();
                    }
                    inFlight.decrementAndGet();
                    semaphore.release();
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(maxObserved.get())
                .as("concurrent holders never exceed total permits")
                .isLessThanOrEqualTo(totalPermits);
        assertThat(acquireCount.get()).isEqualTo(threadCount * iterationsPerThread);
        // 守恒：全部归还后可用许可恢复为总数，且无残留等待
        assertThat(semaphore.getAvailablePermits()).isEqualTo(totalPermits);
        assertThat(semaphore.getWaitingThreadCount()).isZero();
        assertThat(semaphore.getTotalAcquisitions())
                .isEqualTo((long) threadCount * iterationsPerThread);
    }

    @Test
    @Timeout(120)
    void conservationHoldsWithMixedTimeoutsAndInterrupts() throws Exception {
        int totalPermits = 3;
        int threadCount = 12;
        int iterationsPerThread = 200;
        FairSemaphore semaphore = new FairSemaphore(totalPermits);

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        CountDownLatch phaseOneDone = new CountDownLatch(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // 主线程先占住全部许可，阶段一的短超时获取必然超时：验证超时路径不消耗许可
        for (int i = 0; i < totalPermits; i++) {
            semaphore.acquire();
        }
        for (int t = 0; t < threadCount; t++) {
            futures.add(pool.submit(() -> {
                // 阶段一：许可全被主线程持有，tryAcquire 必然超时且不消耗许可
                for (int i = 0; i < 50; i++) {
                    try {
                        semaphore.tryAcquire(2, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                phaseOneDone.countDown();
                // 阶段二：许可已归还，正常获取/归还，验证并发守恒
                for (int i = 0; i < iterationsPerThread; i++) {
                    try {
                        if (!semaphore.tryAcquire(5, TimeUnit.SECONDS)) {
                            continue; // 超时路径：不得消耗许可
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    int now = inFlight.incrementAndGet();
                    maxObserved.accumulateAndGet(now, Math::max);
                    Thread.yield();
                    inFlight.decrementAndGet();
                    semaphore.release();
                }
            }));
        }
        phaseOneDone.await(); // 所有工作线程完成超时阶段后才归还许可，无竞态
        for (int i = 0; i < totalPermits; i++) {
            semaphore.release();
        }
        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(maxObserved.get()).isLessThanOrEqualTo(totalPermits);
        assertThat(semaphore.getAvailablePermits()).isEqualTo(totalPermits);
        assertThat(semaphore.getWaitingThreadCount()).isZero();
        assertThat(semaphore.getTimeoutCount()).isGreaterThan(0L); // 确实覆盖了超时路径
    }

    // ------------------------------------------------------------------
    // 7) 统计指标
    // ------------------------------------------------------------------

    @Test
    @Timeout(30)
    void statisticsAreTracked() throws Exception {
        FairSemaphore semaphore = new FairSemaphore(1);
        semaphore.acquire(); // 快速路径：不排队

        Thread waiter = new Thread(semaphore::acquire);
        waiter.start();
        awaitCondition(() -> semaphore.getWaitingThreadCount() == 1, "waiter enqueued");

        Thread.sleep(80); // 让等待者可观测地等待一段时间
        semaphore.release();
        waiter.join();
        semaphore.release();

        FairSemaphore.Stats stats = semaphore.getStats();
        assertThat(stats.totalPermits()).isEqualTo(1);
        assertThat(stats.availablePermits()).isEqualTo(1);
        assertThat(stats.waitingThreads()).isZero();
        assertThat(stats.totalAcquisitions()).isEqualTo(2);
        assertThat(stats.timeoutCount()).isZero();
        assertThat(stats.averageWaitMillis()).isGreaterThan(0.0);
        assertThat(semaphore.getAverageWaitTimeMillis())
                .isCloseTo(stats.averageWaitMillis(), within(1.0));
    }
}
