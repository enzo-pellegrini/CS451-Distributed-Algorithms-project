package cs451.Printer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

public class LatticeOutputWriter extends Printer {
    public LatticeOutputWriter(String outputPath) throws IOException {
        super(new FileWriter(outputPath));
    }

    public void decided(Collection<Integer> values) {
        try {
            // Print each value in values separated by a space and followed by a unix newline
            if (!values.isEmpty()) {
                String out = String.join(" ", values.stream().map(v -> Integer.toString(v)).toArray(String[]::new));
                println(out);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error while writing to output file");
        }
    }
}
