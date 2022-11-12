package cs451.Parser;

import java.util.List;

@SuppressWarnings("unused")
public class PerfectLinksParser extends Parser {
    private final PerfectLinksConfigParser perfectLinksConfigParser = new PerfectLinksConfigParser();

    public PerfectLinksParser(String[] args) {
        super(args);
    }

    @Override
    public void parse() {
        super.parse();

        if (!perfectLinksConfigParser.populate(super.config(), super.hostsParser)) {
            super.help();
        }
    }

    public List<PerfectLinksConfigParser.ConfigEntry> configEntries() {
        return perfectLinksConfigParser.getEntries();
    }
}
