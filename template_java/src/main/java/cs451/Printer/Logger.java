package cs451.Printer;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class Logger extends Printer {
    private final String prefix;
    private int count;

    public Logger(String prefix) {
        super(new OutputStreamWriter(System.out));
        this.prefix = prefix;
    }

    public synchronized void log(String message) {
        try {
            println(prefix + "[" + ++count + "]: " + message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
