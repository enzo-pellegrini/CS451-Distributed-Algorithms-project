package cs451.Parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FIFOConfigParser extends ConfigParser {
    private int numMessages;

    @Override
    public boolean populate(String path) {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line = br.readLine();
            if (line == null) {
                System.err.println("Problem with the config file!");
                return false;
            }

            numMessages = Integer.parseInt(line);
        } catch (IOException e) {
            System.err.println("Problem with the config file!");
            return false;
        }

        return true;
    }

    public int getNumMessages() {
        return numMessages;
    }
}
