package cs451.Printer;

import java.io.FileWriter;
import java.io.IOException;

public class OutputWriter extends Printer {
    public OutputWriter(String outputPath) throws IOException {
        super(new FileWriter(outputPath));
    }

    public void delivered(int sender, int seqNr) {
        try {
            println("d " + sender + " " + seqNr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void broadcasted(int seqNr) {
        try {
            println("b " + seqNr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
