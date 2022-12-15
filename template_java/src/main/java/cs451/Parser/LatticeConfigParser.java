package cs451.Parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LatticeConfigParser extends ConfigParser {
    private int p;
    private int vs;
    private int ds;
    // private List<List<Integer>> proposals;
    private BufferedReader br;
    private int currentProposal = 0;

    @Override
    public boolean populate(String path) {
        try {
            br = new BufferedReader(new FileReader(path));

            // Read p, vs, ds
            String line = br.readLine();
            if (line == null) {
                System.err.println("Problem with the config file!");
                return false;
            }

            String[] parts = line.split(" ");
            if (parts.length != 3) {
                System.err.println("Problem with the config file!");
                return false;
            }

            p = Integer.parseInt(parts[0]);
            vs = Integer.parseInt(parts[1]);
            ds = Integer.parseInt(parts[2]);
        } catch (IOException e) {
            System.err.println("Problem with the config file!");
            return false;
        }

        return true;
    }

    public int getP() {
        return p;
    }

    public int getVs() {
        return vs;
    }

    public int getDs() {
        return ds;
    }

    public List<Integer> getNextProposal() {
        if (currentProposal >= p) {
            return null;
        }
        try {
            String line = br.readLine();
            if (line == null) {
                return null;
            }

            String[] parts = line.split(" ");

            if (parts.length > vs) {
                System.err.println("Problem with the config file, too many messages for one proposal!");
                return null;
            }

            List<Integer> proposal = new ArrayList<>();
            for (String part : parts) {
                proposal.add(Integer.parseInt(part));
            }

            return proposal;
        } catch (IOException e) {
            System.err.println("Problem with the config file!");
            return null;
        }
    }
}
