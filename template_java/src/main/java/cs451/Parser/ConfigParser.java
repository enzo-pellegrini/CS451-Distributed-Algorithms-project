package cs451.Parser;

import java.io.File;

public class ConfigParser {
    protected String path;

    public boolean populate(String value) {
        File file = new File(value);
        path = file.getPath();
        return true;
    }

    public String getPath() {
        return path;
    }

}
