package com.example.gsb.semaphore;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * 从零实现的公平（FIFO）信号量，不依赖 {@code java.util.concurrent.Semaphore}。
 *
 * <p>设计要点：
 * <ul>
 *   <li>所有状态变更都在 {@code this} 监视器上同步，等待者在 {@code this} 上
 *       {@code wait()}，用 {@code notifyAll()} 唤醒后重新检查自身状态。</li>
 *   <li>每个等待线程对应一个 {@link Waiter} 节点，按入队顺序（FIFO）获得许可；
 *       队列非空时新来的线程一律排队，不允许“插队抢锁”（no barging）。</li>
 *   <li>释放许可时只从队头开始依次发放，保证先来先服务。</li>
 *   <li>超时或被中断的等待者会从队列中移除；若许可恰好在唤醒竞态中已发给它，
 *       会把该许可归还给后续等待者，保证许可不泄漏、总数守恒。</li>
 * </ul>
 *
 * <p><b>动态缩减许可数</b>：{@link #resize(int)} 允许新总数低于当前已发放数量，
 * 此时 {@code available} 会变为负值。已发放的许可不会被强制收回（持有者继续使用），
 * 缩减“延迟生效”：随着持有者陆续 {@link #release()}，可用许可逐步回升，在
 * 已发放数量降到新总数之前不会再发放任何新许可。
 */
public class FairSemaphore {

    /** 等待节点：许可的发放就是把对应节点的 granted 置为 true。 */
    private static final class Waiter {
        boolean granted;
    }

    /** 总许可数（动态调整后可能小于已发放数量）。 */
    private int maxPermits;

    /** 可用许可数。通常满足 0 <= available <= maxPermits；缩减后可能为负。 */
    private int available;

    /** FIFO 等待队列。 */
    private final Deque<Waiter> queue = new ArrayDeque<>();

    private long totalAcquisitions;
    private long timeoutCount;
    private long totalWaitNanos;

    /**
     * @param permits 初始总许可数，必须为正
     */
    public FairSemaphore(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be positive: " + permits);
        }
        this.maxPermits = permits;
        this.available = permits;
    }

    /**
     * 获取一个许可（可中断）。
     *
     * <p>队列中没有更早的等待者且有可用许可时立即返回；否则入队 FIFO 等待。
     * 等待期间被中断会抛出 {@link InterruptedException}，已从队列移除且不消耗许可。
     */
    public void acquire() throws InterruptedException {
        long startNanos = System.nanoTime();
        synchronized (this) {
            if (queue.isEmpty() && available > 0) {
                grantImmediateLocked(startNanos);
                return;
            }
            Waiter waiter = new Waiter();
            queue.addLast(waiter);
            try {
                while (!waiter.granted) {
                    wait();
                }
            } catch (InterruptedException e) {
                // 中断后不消耗许可、不残留等待记录；
                // 若许可恰在竞态中已发到本节点，则归还并转发给后续等待者。
                cleanupAfterAbandonmentLocked(waiter);
                throw e;
            }
            totalAcquisitions++;
            totalWaitNanos += System.nanoTime() - startNanos;
        }
    }

    /**
     * 在指定时间内尝试获取一个许可（可中断、可超时）。
     *
     * @return 成功获取返回 true；超时返回 false（已移出队列、未消耗许可）
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new IllegalArgumentException("unit must not be null");
        }
        long waitNanos = unit.toNanos(timeout);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + waitNanos;

        synchronized (this) {
            if (queue.isEmpty() && available > 0) {
                grantImmediateLocked(startNanos);
                return true;
            }
            if (waitNanos <= 0) {
                timeoutCount++;
                return false;
            }

            Waiter waiter = new Waiter();
            queue.addLast(waiter);
            long remaining = waitNanos;
            try {
                while (!waiter.granted) {
                    if (remaining <= 0) {
                        queue.remove(waiter);
                        timeoutCount++;
                        return false;
                    }
                    long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
                    int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
                    wait(millis, nanos);
                    remaining = deadlineNanos - System.nanoTime();
                }
                totalAcquisitions++;
                totalWaitNanos += System.nanoTime() - startNanos;
                return true;
            } catch (InterruptedException e) {
                cleanupAfterAbandonmentLocked(waiter);
                throw e;
            }
        }
    }

    /** 非阻塞尝试获取，队列为空且有许可时立即成功，否则立即失败。 */
    public synchronized boolean tryAcquire() {
        if (queue.isEmpty() && available > 0) {
            grantImmediateLocked(System.nanoTime());
            return true;
        }
        return false;
    }

    /**
     * 释放一个许可，并按 FIFO 顺序唤醒队头等待者。
     *
     * @throws IllegalStateException 释放数超过获取数（当前没有已发放许可可还）
     */
    public synchronized void release() {
        if (available >= maxPermits) {
            throw new IllegalStateException("release without a matching acquire");
        }
        available++;
        grantFromQueueLocked();
    }

    /**
     * 动态调整总许可数。
     *
     * <p>增加总数会立刻让等待者按 FIFO 获得新许可；减少到低于当前已发放数量时，
     * {@link #availablePermits()} 暂时为负，已发许可不强制收回，待归还后缩减才完全生效。
     *
     * @param newMaxPermits 新的总许可数（非负，0 表示暂时完全关闭）
     */
    public synchronized void resize(int newMaxPermits) {
        if (newMaxPermits < 0) {
            throw new IllegalArgumentException("newMaxPermits must be non-negative: " + newMaxPermits);
        }
        int issued = maxPermits - available;
        maxPermits = newMaxPermits;
        available = newMaxPermits - issued;
        grantFromQueueLocked();
    }

    /** 当前可用许可数；缩减后、归还完成前可能为负（表示超额发放尚未还清）。 */
    public synchronized int availablePermits() {
        return available;
    }

    /** 当前总许可数。 */
    public synchronized int maxPermits() {
        return maxPermits;
    }

    /** 当前已发放（被持有）的许可数。 */
    public synchronized int issuedPermits() {
        return maxPermits - available;
    }

    /** 等待队列长度。 */
    public synchronized int queueLength() {
        return queue.size();
    }

    /** 成功获取许可的总次数。 */
    public synchronized long totalAcquisitions() {
        return totalAcquisitions;
    }

    /** 超时失败的总次数。 */
    public synchronized long timeoutCount() {
        return timeoutCount;
    }

    /**
     * 平均等待时间（毫秒）。立即成功的获取等待时间记为 0。
     */
    public synchronized double averageWaitTimeMillis() {
        if (totalAcquisitions == 0) {
            return 0.0;
        }
        double totalMillis = totalWaitNanos / 1_000_000.0;
        return totalMillis / totalAcquisitions;
    }

    /** 调用方已持有 this 锁且满足“队列为空、available > 0”时直接发一个许可。 */
    private void grantImmediateLocked(long startNanos) {
        available--;
        totalAcquisitions++;
        totalWaitNanos += System.nanoTime() - startNanos;
    }

    /**
     * 从队头开始、按 FIFO 把可用许可发完。
     * 只有调用方持有 this 锁时才可调用。
     */
    private void grantFromQueueLocked() {
        boolean grantedAny = false;
        while (available > 0 && !queue.isEmpty()) {
            Waiter waiter = queue.pollFirst();
            waiter.granted = true;
            available--;
            grantedAny = true;
        }
        if (grantedAny) {
            notifyAll();
        }
    }

    /**
     * 等待者放弃等待（超时除外的中断路径）。若在竞态中许可已发给它，
     * 归还该许可并转发给后续等待者；否则仅从队列移除。
     */
    private void cleanupAfterAbandonmentLocked(Waiter waiter) {
        if (waiter.granted) {
            available++;
            grantFromQueueLocked();
        } else {
            queue.remove(waiter);
        }
    }
}
