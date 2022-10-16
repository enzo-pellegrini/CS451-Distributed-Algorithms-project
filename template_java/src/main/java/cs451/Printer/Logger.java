package cs451.Printer;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

public class Logger extends Printer {
    public Logger() {
        super(new BufferedWriter(new OutputStreamWriter(System.out)));
    }

    public void println(String message) {
        print(message + "\n");
    }
}
