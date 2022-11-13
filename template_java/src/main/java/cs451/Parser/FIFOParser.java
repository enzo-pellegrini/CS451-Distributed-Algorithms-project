package cs451.Parser;

public class FIFOParser extends Parser {
    private final FIFOConfigParser fifoConfigParser = new FIFOConfigParser();

    public FIFOParser(String[] args) {
        super(args);
    }

    @Override
    public void parse() {
        super.parse();

        if (!fifoConfigParser.populate(super.config())) {
            super.help();
        }
    }

    public int numMessages() {
        return fifoConfigParser.getNumMessages();
    }
}
