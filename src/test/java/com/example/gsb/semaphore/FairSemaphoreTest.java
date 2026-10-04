package com.example.gsb.semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class FairSemaphoreTest {

    private static void awaitTrue(BooleanSupplier condition, String description) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + description);
            }
            Thread.sleep(5);
        }
    }

    private static Thread startDaemon(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    // ---------- 1) 基本获取与释放 ----------

    @Test
    void acquireAndReleaseUpdateAvailablePermits() throws InterruptedException {
        FairSemaphore sem = new FairSemaphore(2);

        sem.acquire();
        assertThat(sem.availablePermits()).isEqualTo(1);
        assertThat(sem.issuedPermits()).isEqualTo(1);

        sem.acquire();
        assertThat(sem.availablePermits()).isEqualTo(0);

        sem.release();
        sem.release();
        assertThat(sem.availablePermits()).isEqualTo(2);
        assertThat(sem.issuedPermits()).isEqualTo(0);
        assertThat(sem.totalAcquisitions()).isEqualTo(2);
    }

    @Test
    void rejectsInvalidPermitCounts() {
        assertThatThrownBy(() -> new FairSemaphore(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FairSemaphore(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void overReleaseThrows() throws InterruptedException {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire();
        sem.release();
        assertThatThrownBy(sem::release).isInstanceOf(IllegalStateException.class);
    }

    // ---------- 2) 公平性：FIFO，无长期插队 ----------

    @Test
    void waitersAreServedInFifoOrder() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire(); // 主线程先占住许可

        int waiterCount = 5;
        List<Integer> acquisitionOrder = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < waiterCount; i++) {
            int index = i;
            Thread t = startDaemon(() -> {
                try {
                    sem.acquire();
                    acquisitionOrder.add(index);
                    Thread.sleep(30); // 持有一小段时间，确保后续线程都已入队
                    sem.release();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            threads.add(t);
            // 等该线程确实入队后再启动下一个，保证入队顺序 = 启动顺序
            int expectedQueueLength = i + 1;
            awaitTrue(() -> sem.queueLength() == expectedQueueLength, "waiter " + index + " enqueued");
        }

        sem.release(); // 放行，等待者应依次获得许可

        for (Thread t : threads) {
            t.join(10_000);
            assertThat(t.isAlive()).isFalse();
        }
        assertThat(acquisitionOrder).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void newArrivalCannotBargeAheadOfQueuedWaiters() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire();

        CountDownLatch acquired = new CountDownLatch(1);
        Thread waiter = startDaemon(() -> {
            try {
                sem.acquire();
                acquired.countDown();
                Thread.sleep(50);
                sem.release();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitTrue(() -> sem.queueLength() == 1, "waiter enqueued");

        sem.release(); // 许可直接发放给队头等待者

        // 即使等待者还没来得及被调度，新来者也不允许抢走已定向发放的许可
        assertThat(sem.tryAcquire()).isFalse();
        assertThat(sem.tryAcquire(0, TimeUnit.MILLISECONDS)).isFalse();

        assertThat(acquired.await(10, TimeUnit.SECONDS)).isTrue();
        waiter.join(10_000);
        assertThat(sem.availablePermits()).isEqualTo(1);
    }

    // ---------- 3) 超时获取：移除队列、不消耗许可 ----------

    @Test
    void timedAcquireExpiresAndLeavesNoResidue() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire(); // 占住唯一许可

        AtomicReference<Boolean> result = new AtomicReference<>();
        Thread t = startDaemon(() -> {
            try {
                result.set(sem.tryAcquire(150, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitTrue(() -> sem.queueLength() == 1, "waiter enqueued");

        t.join(10_000);
        assertThat(result.get()).isFalse();
        assertThat(sem.queueLength()).isZero();          // 已从等待队列移除
        assertThat(sem.timeoutCount()).isEqualTo(1);
        assertThat(sem.availablePermits()).isZero();     // 许可仍归主线程持有，未被消耗

        sem.release();
        assertThat(sem.availablePermits()).isEqualTo(1); // 许可守恒：没有泄漏也没有虚增
        assertThat(sem.tryAcquire()).isTrue();           // 超时线程没有拿走许可
    }

    @Test
    void zeroTimeoutTryAcquireFailsFastWhenBusy() throws InterruptedException {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire();
        assertThat(sem.tryAcquire(0, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(sem.tryAcquire(10, TimeUnit.NANOSECONDS)).isFalse();
        assertThat(sem.queueLength()).isZero();
        assertThat(sem.timeoutCount()).isEqualTo(2);
    }

    @Test
    void timedAcquireSucceedsBeforeDeadline() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire();

        AtomicReference<Boolean> result = new AtomicReference<>();
        Thread t = startDaemon(() -> {
            try {
                result.set(sem.tryAcquire(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitTrue(() -> sem.queueLength() == 1, "waiter enqueued");
        Thread.sleep(50);
        sem.release();

        t.join(10_000);
        assertThat(result.get()).isTrue();
        assertThat(sem.timeoutCount()).isZero();
        assertThat(sem.issuedPermits()).isEqualTo(1);
    }

    // ---------- 4) 动态调整总许可数 ----------

    @Test
    void resizeUpImmediatelyFreesWaitingThreads() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire();

        CountDownLatch acquired = new CountDownLatch(1);
        Thread waiter = startDaemon(() -> {
            try {
                sem.acquire();
                acquired.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitTrue(() -> sem.queueLength() == 1, "waiter enqueued");

        sem.resize(3); // 扩容后等待者应立即获得许可，无需等待归还
        assertThat(acquired.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(sem.maxPermits()).isEqualTo(3);
        assertThat(sem.availablePermits()).isEqualTo(1); // 3 - 2 issued
        waiter.join(10_000);
    }

    @Test
    void resizeBelowIssuedTakesEffectLazilyAsPermitsAreReturned() throws InterruptedException {
        FairSemaphore sem = new FairSemaphore(3);
        sem.acquire();
        sem.acquire();
        sem.acquire();
        assertThat(sem.issuedPermits()).isEqualTo(3);

        sem.resize(1); // 新总数低于已发放数：已发许可不强制收回
        assertThat(sem.maxPermits()).isEqualTo(1);
        assertThat(sem.availablePermits()).isEqualTo(-2); // 超额 2 个待归还
        assertThat(sem.tryAcquire()).isFalse();

        sem.release();
        assertThat(sem.availablePermits()).isEqualTo(-1);
        assertThat(sem.tryAcquire()).isFalse(); // 仍未降到新上限，不发放新许可

        sem.release();
        assertThat(sem.availablePermits()).isZero();
        assertThat(sem.tryAcquire()).isFalse();

        sem.release(); // 全部归还，缩减完全生效
        assertThat(sem.availablePermits()).isEqualTo(1);
        assertThat(sem.tryAcquire()).isTrue();
        assertThat(sem.issuedPermits()).isEqualTo(1);
    }

    @Test
    void resizeRejectsNegativeTotal() {
        FairSemaphore sem = new FairSemaphore(1);
        assertThatThrownBy(() -> sem.resize(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    // ---------- 5) 可中断获取 ----------

    @Test
    void interruptedAcquireLeavesNoPermitOrQueueResidue() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire();

        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = startDaemon(() -> {
            try {
                sem.acquire();
            } catch (InterruptedException e) {
                error.set(e);
            }
        });
        awaitTrue(() -> sem.queueLength() == 1, "waiter enqueued");

        t.interrupt();
        t.join(10_000);

        assertThat(error.get()).isInstanceOf(InterruptedException.class);
        assertThat(sem.queueLength()).isZero();      // 无残留等待记录
        assertThat(sem.availablePermits()).isZero(); // 许可仍归主线程，未被消耗
        assertThat(sem.totalAcquisitions()).isEqualTo(1); // 仅主线程的一次获取

        sem.release();
        assertThat(sem.availablePermits()).isEqualTo(1); // 许可守恒
    }

    @Test
    void interruptedTimedAcquireAlsoCleansUp() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        sem.acquire();

        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = startDaemon(() -> {
            try {
                sem.tryAcquire(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                error.set(e);
             }
        });
        awaitTrue(() -> sem.queueLength() == 1, "waiter enqueued");

        t.interrupt();
        t.join(10_000);

        assertThat(error.get()).isInstanceOf(InterruptedException.class);
        assertThat(sem.queueLength()).isZero();
        assertThat(sem.timeoutCount()).isZero(); // 中断不算超时
        sem.release();
        assertThat(sem.availablePermits()).isEqualTo(1);
    }

    // ---------- 6) 并发正确性：许可守恒、发放数不超上限 ----------

    @Test
    void permitsAreConservedUnderConcurrency() throws Exception {
        int permits = 3;
        int threads = 8;
        int iterations = 300;
        FairSemaphore sem = new FairSemaphore(permits);

        AtomicInteger holders = new AtomicInteger();
        AtomicInteger maxHolders = new AtomicInteger();
        AtomicBoolean violation = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Runnable worker = () -> {
            for (int i = 0; i < iterations; i++) {
                try {
                    sem.acquire();
                } catch (InterruptedException e) {
                    failure.compareAndSet(null, e);
                    return;
                }
                int now = holders.incrementAndGet();
                maxHolders.accumulateAndGet(now, Math::max);
                if (now > permits) {
                    violation.set(true);
                }
                Thread.yield();
                holders.decrementAndGet();
                sem.release();
            }
        };

        List<Thread> pool = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.add(startDaemon(worker));
        }
        for (Thread t : pool) {
            t.join(20_000);
            assertThat(t.isAlive()).isFalse();
        }

        assertThat(failure.get()).isNull();
        assertThat(violation.get()).isFalse();                    // 任何时刻已发放数 <= 总许可数
        assertThat(maxHolders.get()).isLessThanOrEqualTo(permits);
        assertThat(maxHolders.get()).isGreaterThan(1);            // 确实发生了并发持有
        assertThat(sem.availablePermits()).isEqualTo(permits);    // 许可总数守恒
        assertThat(sem.issuedPermits()).isZero();
        assertThat(sem.queueLength()).isZero();
        assertThat(sem.totalAcquisitions()).isEqualTo((long) threads * iterations);
    }

    @Test
    void mixedTimedAndUntimedAcquiresConservePermits() throws Exception {
        FairSemaphore sem = new FairSemaphore(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Runnable worker = () -> {
            for (int i = 0; i < 200; i++) {
                try {
                    if (i % 2 == 0) {
                        sem.acquire();
                    } else if (!sem.tryAcquire(5, TimeUnit.SECONDS)) {
                        failure.compareAndSet(null, new AssertionError("unexpected timeout"));
                        return;
                    }
                    sem.release();
                } catch (InterruptedException e) {
                    failure.compareAndSet(null, e);
                    return;
                }
            }
        };

        List<Thread> pool = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            pool.add(startDaemon(worker));
        }
        for (Thread t : pool) {
            t.join(20_000);
            assertThat(t.isAlive()).isFalse();
        }

        assertThat(failure.get()).isNull();
        assertThat(sem.availablePermits()).isEqualTo(2);
        assertThat(sem.queueLength()).isZero();
        assertThat(sem.totalAcquisitions()).isEqualTo(6L * 200);
    }

    // ---------- 7) 统计信息 ----------

    @Test
    void statisticsTrackAcquisitionsTimeoutsAndWaitTime() throws Exception {
        FairSemaphore sem = new FairSemaphore(1);
        assertThat(sem.averageWaitTimeMillis()).isZero();

        sem.acquire(); // 立即成功，等待时间记 0
        assertThat(sem.totalAcquisitions()).isEqualTo(1);

        AtomicReference<Boolean> queued = new AtomicReference<>();
        Thread waiter = startDaemon(() -> {
            try {
                queued.set(sem.tryAcquire(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitTrue(() -> sem.queueLength() == 1, "waiter enqueued");
        assertThat(sem.queueLength()).isEqualTo(1);
        Thread.sleep(80); // 让等待者实际等一会儿
        sem.release();
        waiter.join(10_000);

        assertThat(queued.get()).isTrue();
        assertThat(sem.totalAcquisitions()).isEqualTo(2);
        assertThat(sem.averageWaitTimeMillis()).isGreaterThan(0.0);

        waiter = startDaemon(() -> {
            try {
                sem.tryAcquire(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.join(10_000);
        assertThat(sem.timeoutCount()).isEqualTo(1);
        assertThat(sem.queueLength()).isZero();
    }
}
