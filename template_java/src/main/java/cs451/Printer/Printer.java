package cs451.Printer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Printer {
    protected static final int SLEEP_PERIOD = 10;
    protected final BufferedWriter writer;
    protected final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    protected final AtomicBoolean running = new AtomicBoolean(true);

    protected Printer(BufferedWriter bw) {
        this.writer = bw;

        Thread t = new Thread(() -> {
            while (running.get()) {
                String curr = queue.poll();
                while (curr != null && running.get()) {
                    try {
                        writer.write(curr);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    curr = queue.poll();
                }
            }

            try {
                Thread.sleep(SLEEP_PERIOD);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
    }

    protected void print(String text) {
        queue.offer(text);
    }

    /**
     * Stop writing to ouput stream
     */
    public void poison() {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        running.set(false);
    }
}
