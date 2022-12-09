package cs451.Printer;

import java.io.FileWriter;
import java.io.IOException;


public class BroadcastOutputWriter extends Printer {
    public BroadcastOutputWriter(String outputPath) throws IOException {
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
            e.printStackTrace();
            System.out.println("Error while writing to output file");
        }
    }
}
