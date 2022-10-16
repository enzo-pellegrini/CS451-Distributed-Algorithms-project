package cs451.Printer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OutputWriter extends Printer {
    public OutputWriter(String outputPath) throws IOException {
        super(new BufferedWriter(new FileWriter(outputPath)));
    }

    public void delivered(int sender, int seqNr) {
        print("d " + sender + " " + seqNr + "\n");
    }

    public void broadcasted(int seqNr) {
        print("b " + seqNr + "\n");
    }
}
