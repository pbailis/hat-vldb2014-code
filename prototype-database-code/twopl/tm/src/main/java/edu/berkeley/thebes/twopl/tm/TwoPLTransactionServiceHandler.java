package edu.berkeley.thebes.twopl.tm;

import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.ThebesTwoPLTransactionClient;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;
import edu.berkeley.thebes.twopl.tm.SimpleStackOperationInterpreter.Function;
import edu.berkeley.thebes.twopl.tm.SimpleStackOperationInterpreter.StatementNode;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import javax.naming.ConfigurationException;

public class TwoPLTransactionServiceHandler implements TwoPLTransactionService.Iface {
    private ThebesTwoPLTransactionClient client;

    public TwoPLTransactionServiceHandler(ThebesTwoPLTransactionClient client) {
        this.client = client;
    }

    @Override
    public synchronized TwoPLTransactionResult execute(List<String> transaction) throws TException {
    	System.out.println("NEW VERSION");
    	ThebesTwoPLTransactionClient client = new ThebesTwoPLTransactionClient();
    	try {
			client.open();
		} catch (ConfigurationException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
        SimpleStackOperationInterpreter interpreter =
                new SimpleStackOperationInterpreter(client);
        
        // Parse the transaction and determine which keys are read to & written from
        List<StatementNode> statements = Lists.newArrayList();
        Set<String> readKeys = Sets.newHashSet();
        Set<String> writeKeys = Sets.newHashSet();
        for (String operation : transaction) {
        	if (operation.startsWith("put")) {
        		String[] x = operation.split(" ");
        		readKeys.remove(x[1]);
        		writeKeys.add(x[1]);
        	} else {
        		String key = operation.substring(operation.indexOf(" ")+1);
                if (!writeKeys.contains(key)) {
                    readKeys.add(key);
                }
        	}
        	/*
            StatementNode statement = interpreter.parse(operation);
            String key = statement.getTarget();
            
            if (statement.getFunction() == Function.PUT) {
                readKeys.remove(key);
                writeKeys.add(key);
            } else if (statement.getFunction() == Function.GET) {
                if (!writeKeys.contains(key)) {
                    readKeys.add(key);
                }
            }
            
            statements.add(statement);
            */
        }
        
        // Now actually execute commands, starting by locking everything we need.
        client.beginTransaction();
        try {
            for (String writeKey : writeKeys) {
                client.writeLock(writeKey);
            }
            for (String readKey : readKeys) {
                client.readLock(readKey);
            }
            for (String operation : transaction) {
            	if (operation.startsWith("put")) {
            		String[] x = operation.split(" ");
            		readKeys.remove(x[1]);
            		writeKeys.add(x[1]);
            		client.put(x[1], ByteBuffer.wrap(x[2].getBytes()));
            	} else {
            		String[] x = operation.split(" ");
                    client.get(x[1]);
            	}
            }
            client.commitTransaction();
            /*
            for (StatementNode statement : statements) {
                // TODO: Clean up interpreter by having it merely evaluate, not execute.
                interpreter.execute(statement);
            }*/
        } catch (AssertionError e) {
            e.printStackTrace();
            throw new TTransactionAbortedException(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.abortTransaction();
        }
        
        return new TwoPLTransactionResult(interpreter.getOutput());
    }
}