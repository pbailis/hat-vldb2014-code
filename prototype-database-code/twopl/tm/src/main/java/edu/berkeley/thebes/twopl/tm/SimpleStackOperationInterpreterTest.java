package edu.berkeley.thebes.twopl.tm;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Map;

import javax.naming.ConfigurationException;

import junit.framework.TestCase;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.interfaces.IThebesClient;

public class SimpleStackOperationInterpreterTest extends TestCase {
    FakeClient client;
    SimpleStackOperationInterpreter interpreter;
    
    @Override
    public void setUp() {
        client = new FakeClient();
        try {
            client.open();
            interpreter = new SimpleStackOperationInterpreter(client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private ByteBuffer execute(String str) throws TException {
        return interpreter.execute(interpreter.parse(str));
    }
    
    public void testBasic() {
        try {
            assertEquals(execute("put x 3"), fromInt(3));
            assertEquals(client.memory.get("x"), fromInt(3));
            assertEquals(execute("get x"), fromInt(3));
        } catch (TException e) {
            e.printStackTrace();
        }
    }
    
    public void test1() {
        try {
            assertEquals(execute("put x + 1 2"), fromInt(3));
            assertEquals(execute("put y * x + x 1"), fromInt(12));
            assertEquals(execute("put y + y 1"), fromInt(13));
            assertEquals(execute("get x"), fromInt(3));
            assertEquals(execute("get y"), fromInt(13));
        } catch (TException e) {
            e.printStackTrace();
        }
    }
    
    public void test2() {
        try {
            client.memory.put("y", fromInt(7));
            assertEquals(execute("put x 1"), fromInt(1));
            assertEquals(execute("get y"), fromInt(7));
            assertEquals(execute("put x + x y"), fromInt(8));
        } catch (TException e) {
            e.printStackTrace();
        }
    }
    
    public void testUseUnreadValue() {
        try {
            client.memory.put("y", fromInt(7));
            assertEquals(execute("put x 1"), fromInt(1));
            try {
                assertEquals(execute("put x + x y"), fromInt(8));
                fail();
            } catch (AssertionError e) { }
        } catch (TException e) {
            e.printStackTrace();
        }
    }
    
    public void testBadSyntax() {
        try {
            String[] badCommands = {
                    "put x",
                    "put x 3 4",
                    "get",
                    "write x",
                    "put x + 1",
                    "put x + 1 2 3",
                    "put x + 1 +",
                    "put + 1 x",
                    "put 1 x",
                    "+ 1 2",   // root must be statement (put/get)
                    "1",
                    "x",
                    "get 1",   // not a variable: 1
                    "get abc", // unknown value: abc
                    "put z z", // unknown value: z
            };
            for (String cmd : badCommands) {
                try {
                    System.out.println("Executing: " + cmd);
                    execute(cmd);
                    fail("Should've failed on: " + cmd);
                } catch (AssertionError e) { }
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }
    
    private ByteBuffer fromInt(int i) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        bb.rewind();
        return bb;
    }
    
    
    private class FakeClient implements IThebesClient {
        private Map<String, ByteBuffer> memory = Maps.newHashMap();

        @Override
        public boolean put(String key, ByteBuffer value) throws TException {
            memory.put(key, value);
            return true;
        }

        @Override
        public ByteBuffer get(String key) throws TException {
            return memory.get(key);
        }

        @Override
        public void open() throws TTransportException, ConfigurationException,
                FileNotFoundException {
        }

        @Override
        public void beginTransaction() throws TException {
        }

        @Override
        public boolean commitTransaction() throws TException {
            return true;
        }

        @Override
        public void abortTransaction() throws TException {
        }


        @Override
        public void sendCommand(String cmd) throws TException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
