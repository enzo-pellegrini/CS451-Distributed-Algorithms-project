package cs451.Printer;

import java.io.IOException;
import java.io.OutputStreamWriter;

public abstract class Printer {
    protected final OutputStreamWriter writer;

    protected Printer(OutputStreamWriter writer) {
        this.writer = writer;
    }

    protected void print(String text) throws IOException {
        writer.write(text);
    }

    protected void println(String text) throws IOException {
        print(text+'\n');
    }

    public void flush() throws IOException {
        writer.flush();
        writer.close();
    }
}
