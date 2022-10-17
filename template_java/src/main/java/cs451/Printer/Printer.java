package cs451.Printer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public abstract class Printer {
    protected final BufferedWriter writer;

    protected Printer(OutputStreamWriter writer) {
        this.writer = new BufferedWriter(writer);
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
