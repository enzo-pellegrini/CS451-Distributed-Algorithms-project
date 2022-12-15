package cs451.Parser;

import java.io.IOException;
import java.util.List;

public class LatticeParser extends Parser {
    private final LatticeConfigParser latticeConfigParser = new LatticeConfigParser();

    public LatticeParser(String[] args) {
        super(args);
    }

    @Override
    public void parse() {
        super.parse();

        if (!latticeConfigParser.populate(super.config())) {
            super.help();
        }
    }

    public int getP() {
        return latticeConfigParser.getP();
    }

    public int getVs() {
        return latticeConfigParser.getVs();
    }

    public int getDs() {
        return latticeConfigParser.getDs();
    }

    public List<Integer> getNextProposal() {
        return latticeConfigParser.getNextProposal();
    }
}
