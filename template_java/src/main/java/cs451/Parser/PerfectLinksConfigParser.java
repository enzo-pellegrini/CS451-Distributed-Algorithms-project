package cs451.Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PerfectLinksConfigParser {
    public class ConfigEntry {
        private final int numMessages;
        private final int dstProcess;

        public ConfigEntry(int numMessages, int dstProcess) {
            this.numMessages = numMessages;
            this.dstProcess = dstProcess;
        }
        public int getNumMessages() {
            return numMessages;
        }

        public int getDstProcess() {
            return dstProcess;
        }
    }

    private List<ConfigEntry> entries = new ArrayList<>();

    public boolean populate(String path, HostsParser hostsParser) {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            int lineNum = 1;
            for (String line; (line = br.readLine()) != null; lineNum++) {
                if (line.isBlank()) {
                    continue;
                }

                String[] splits = line.split("\\s+");
                if (splits.length != 2) {
                    System.err.println("Problem with the line " + lineNum + " in the config file!");
                    return false;
                }

                int numMessages = Integer.parseInt(splits[0]);
                int dstProcess = Integer.parseInt(splits[1]);

                if (!hostsParser.inRange(dstProcess)) {
                    System.err.println("Problem with the line " + lineNum + " in the config file, process " + dstProcess + " does not exist!");
                    return false;
                }

                entries.add(new ConfigEntry(numMessages, dstProcess));
            }
        } catch (IOException e) {
            System.err.println("Problem with the config file!");
            return false;
        }

        return true;
    }

    public List<ConfigEntry> getEntries() {
        return entries;
    }
}
