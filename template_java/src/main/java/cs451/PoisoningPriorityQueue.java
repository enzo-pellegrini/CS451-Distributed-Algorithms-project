package cs451;

import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PoisoningPriorityQueue<T extends Comparable> {
    private final Lock lock = new ReentrantLock();
    private final Condition nonEmpty = lock.newCondition();
    private final PriorityQueue<T> q = new PriorityQueue<>();
    private boolean poisoned = false;

    public PoisoningPriorityQueue() {

    }

    /**
     * Take the head of the queue
     * Waits on a condition variable until the queue has elements in it <b>or it's
     * poisoned</b>
     * 
     * @return either
     *         <ul>
     *         <li>head of the queue
     *         <li>null if queue is poisoned
     *         </ul>
     * @throws InterruptedException
     */
    public T take() throws InterruptedException {
        if (poisoned) {
            return null;
        }

        lock.lockInterruptibly();

        T result = null;
        try {
            while (!poisoned && (result = q.poll()) == null) {
                nonEmpty.await();
            }
        } finally {
            lock.unlock();
        }

        return poisoned ? null : result;
    }

    /**
     * Insert specified element into the queue
     * @param val Value to be inserted
     * @throws IllegalArgumentException Thrown if val is null
     */
    public void offer(T val) throws IllegalArgumentException {
        if (val == null)
            throw new IllegalArgumentException();
        lock.lock();

        try {
            q.offer(val);
            nonEmpty.notify();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Poison the queue and notify all consumers
     * After this call all consumers will return null
     */
    public synchronized void poison() {
        poisoned = true;
        nonEmpty.notifyAll();
    }
}
