package com.example.gsb.semaphore;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * 基于 FIFO 等待队列的公平信号量（不使用 {@code java.util.concurrent.Semaphore}）。
 *
 * <h2>公平性模型</h2>
 * 每个未能立即获得许可的线程都会在尾部入队一个 {@link Waiter} 节点。
 * 许可归还时，{@link #grantToWaiters()} 在锁内直接把许可授予队首节点（出队、
 * 置 granted、计数），再 {@code notifyAll} 唤醒。新到达的线程只要看到队列非空
 * 就必须排队，即使此刻有可用许可也不能"插队"。因此许可严格按 FIFO 转移，
 * 等待者不会长期被后来者饿死。
 *
 * <h2>超时与中断</h2>
 * 等待节点只有两种终态：被授予（granted，已出队）或被等待者自己取消
 * （超时 / 中断，锁内从队列移除）。取消时如果发现节点恰好刚被授予，则按
 * 获得许可处理，保证许可既不会丢失也不会被超时线程意外消耗。
 *
 * <h2>动态缩容</h2>
 * {@link #resize(int)} 减少总许可数时不会撤销已经发放的许可。若新总数低于
 * 当前已发放数，缩容惰性生效：已发出的许可继续有效，持有者正常归还；在
 * "已发放数 &lt;= 新总数"之前不会向等待队列发放任何新许可，全部归还到
 * 阈值以下后自动恢复正常派发。任何时刻"发放动作"都不会使已发放数超过
 * 当前总许可数。
 */
public class FairSemaphore {

    private final Object lock = new Object();

    private int totalPermits;
    private int issuedPermits;
    private final Deque<Waiter> waiters = new ArrayDeque<>();

    private long totalAcquisitions;
    private long timeoutCount;
    private long queuedAcquisitions;
    private long totalQueuedWaitNanos;

    private static final class Waiter {
        private final long enqueueNanos = System.nanoTime();
        private boolean granted;
    }

    /** 创建指定许可总数的公平信号量。 */
    public FairSemaphore(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be >= 1, got " + permits);
        }
        this.totalPermits = permits;
    }

    /**
     * 获取一个许可，不可中断地阻塞直到成功；等待期间被中断会继续排队，
     * 获取成功后恢复中断标记。
     */
    public void acquire() {
        boolean interrupted = false;
        synchronized (lock) {
            if (waiters.isEmpty() && issuedPermits < totalPermits) {
                grantImmediately();
                return;
            }
            Waiter waiter = new Waiter();
            waiters.addLast(waiter);
            while (!waiter.granted) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** 可中断地获取一个许可；被中断时抛出异常且不消耗许可、不残留等待记录。 */
    public void acquireInterruptibly() throws InterruptedException {
        synchronized (lock) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (waiters.isEmpty() && issuedPermits < totalPermits) {
                grantImmediately();
                return;
            }
            Waiter waiter = new Waiter();
            waiters.addLast(waiter);
            while (!waiter.granted) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    if (waiter.granted) {
                        // 唤醒与授予发生在中断之后：许可已归本线程，恢复中断标记后正常返回。
                        Thread.currentThread().interrupt();
                        return;
                    }
                    waiters.remove(waiter);
                    throw e;
                }
            }
        }
    }

    /** 立即尝试获取，队列非空或无可用许可时返回 {@code false}，不排队。 */
    public boolean tryAcquire() {
        synchronized (lock) {
            if (waiters.isEmpty() && issuedPermits < totalPermits) {
                grantImmediately();
                return true;
            }
            return false;
        }
    }

    /**
     * 在指定超时内可中断地获取许可。
     *
     * @return 成功返回 {@code true}；超时返回 {@code false}，此时等待记录已移除且不消耗许可。
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        synchronized (lock) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (waiters.isEmpty() && issuedPermits < totalPermits) {
                grantImmediately();
                return true;
            }
            if (timeout <= 0L) {
                return false;
            }
            Waiter waiter = new Waiter();
            waiters.addLast(waiter);
            for (;;) {
                if (waiter.granted) {
                    return true;
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0L) {
                    waiters.remove(waiter);
                    timeoutCount++;
                    return false;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(lock, remainingNanos);
                } catch (InterruptedException e) {
                    if (waiter.granted) {
                        Thread.currentThread().interrupt();
                        return true;
                    }
                    waiters.remove(waiter);
                    throw e;
                }
            }
        }
    }

    /** 归还一个许可，并在锁内按 FIFO 直接授予队首等待者。 */
    public void release() {
        synchronized (lock) {
            if (issuedPermits <= 0) {
                throw new IllegalStateException("release without a matching acquire");
            }
            issuedPermits--;
            grantToWaiters();
        }
    }

    /**
     * 动态调整总许可数。扩容会立即向队列派发新增许可；缩容不撤销已发放许可，
     * 等待归还到阈值以下后自动恢复派发（惰性缩容）。
     */
    public void resize(int newTotalPermits) {
        if (newTotalPermits < 1) {
            throw new IllegalArgumentException("newTotalPermits must be >= 1, got " + newTotalPermits);
        }
        synchronized (lock) {
            totalPermits = newTotalPermits;
            grantToWaiters();
        }
    }

    public int getTotalPermits() {
        synchronized (lock) {
            return totalPermits;
        }
    }

    /** 当前可立即发放给新请求者的许可数（缩容过渡期最小为 0）。 */
    public int getAvailablePermits() {
        synchronized (lock) {
            return Math.max(0, totalPermits - issuedPermits);
        }
    }

    public int getWaitingThreadCount() {
        synchronized (lock) {
            return waiters.size();
        }
    }

    public long getTotalAcquisitions() {
        synchronized (lock) {
            return totalAcquisitions;
        }
    }

    public long getTimeoutCount() {
        synchronized (lock) {
            return timeoutCount;
        }
    }

    /** 曾排队等待并最终成功的线程的平均等待毫秒数；无人排队过则返回 0。 */
    public double getAverageWaitTimeMillis() {
        synchronized (lock) {
            if (queuedAcquisitions == 0L) {
                return 0.0;
            }
            return ((double) totalQueuedWaitNanos / queuedAcquisitions) / 1_000_000.0;
        }
    }

    /** 一次性获取全部统计指标的快照。 */
    public Stats getStats() {
        synchronized (lock) {
            double avgMillis = queuedAcquisitions == 0L
                    ? 0.0
                    : ((double) totalQueuedWaitNanos / queuedAcquisitions) / 1_000_000.0;
            return new Stats(
                    totalPermits,
                    Math.max(0, totalPermits - issuedPermits),
                    waiters.size(),
                    totalAcquisitions,
                    timeoutCount,
                    avgMillis);
        }
    }

    /** 信号量统计快照。 */
    public record Stats(
            int totalPermits,
            int availablePermits,
            int waitingThreads,
            long totalAcquisitions,
            long timeoutCount,
            double averageWaitMillis) {
    }

    /** 调用时调用线程持有锁，且无排队者、有可用许可。 */
    private void grantImmediately() {
        issuedPermits++;
        totalAcquisitions++;
    }

    /** 把可用许可按 FIFO 顺序直接授予等待节点；调用时必须持有 {@link #lock}。 */
    private void grantToWaiters() {
        boolean grantedAny = false;
        while (!waiters.isEmpty() && issuedPermits < totalPermits) {
            Waiter waiter = waiters.pollFirst();
            waiter.granted = true;
            issuedPermits++;
            totalAcquisitions++;
            queuedAcquisitions++;
            totalQueuedWaitNanos += System.nanoTime() - waiter.enqueueNanos;
            grantedAny = true;
        }
        if (grantedAny) {
            lock.notifyAll();
        }
    }
}
