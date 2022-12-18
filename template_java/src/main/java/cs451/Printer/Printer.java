package cs451.Printer;

import java.io.IOException;
import java.io.OutputStreamWriter;

public abstract class Printer {
    protected final OutputStreamWriter writer;
    int count = 0;

    protected Printer(OutputStreamWriter writer) {
        this.writer = writer;
    }

    protected void print(String text) throws IOException {
        if (count++ > 4) {
            writer.flush();
            count = 0;
        }
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
