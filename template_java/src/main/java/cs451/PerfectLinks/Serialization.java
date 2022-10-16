package cs451.PerfectLinks;

import java.io.*;

/**
 * Contains static methods for serialization and deserialization
 */
class Serialization {

    /**
     * Serialize object to byte array
     *
     * @param data Message to be serialized
     * @return Byte array from message
     * @throws IOException
     */
    static public byte[] serialize(Serializable data) {
        byte[] buf = null;

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(data);
            oos.flush();
            oos.close(); // not sure
            buf = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return buf;
    }

    /**
     * Deserialize object from byte array
     *
     * @param buff buffer to be serialized
     * @return desiarialized object
     * @throws IOException
     */
    static public Object deserialize(byte[] buff) {
        Object out = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(buff);
            ObjectInputStream ois;
            ois = new ObjectInputStream(bais);
            out = ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return out;
    }
}
