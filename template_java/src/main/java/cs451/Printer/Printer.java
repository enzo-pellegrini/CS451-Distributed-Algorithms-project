package cs451.Printer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Printer {
    protected static final int SLEEP_PERIOD = 10;
    protected final BufferedWriter writer;
    protected final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    protected final AtomicBoolean running = new AtomicBoolean(true);

    protected Printer(BufferedWriter bw) {
        this.writer = bw;

        Thread t = new Thread(() -> {
            while (!queue.isEmpty() || running.get()) {
                String curr = null;
                try {
                    curr = queue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    writer.write(curr);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
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
    public void flush() {
        running.set(false);
    }
}
