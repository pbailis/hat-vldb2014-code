package edu.berkeley.thebes.twopl.tm;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Map;

public interface TwoPLOperationInterpreter<T> {    
    /** Parses and executes the given operation! 
     * @return 
     * @throws TException */
    
    T parse(String operation);
    ByteBuffer execute(T op) throws TException;
    
    Map<String, ByteBuffer> getOutput();
}
