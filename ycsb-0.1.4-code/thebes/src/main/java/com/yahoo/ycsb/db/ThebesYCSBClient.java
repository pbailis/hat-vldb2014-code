package com.yahoo.ycsb.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.TransactionalDB;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;

import edu.berkeley.thebes.client.ThebesClient;


public class ThebesYCSBClient extends DB implements TransactionalDB {

    ThebesClient client;

    private final Logger logger = Logger.getLogger(ThebesYCSBClient.class);
    
    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;

    private IntegerGenerator transactionLengthGenerator;
    
    @Override
    public int getNextTransactionLength() {
    	return transactionLengthGenerator.nextInt();
    }
    
	public void init() throws DBException {
        String transactionLengthDistributionType = System.getProperty("transactionLengthDistributionType");
        String transactionLengthDistributionParameter = System.getProperty("transactionLengthDistributionParameter");
        if(transactionLengthDistributionType == null)
            throw new DBException("required transactionLengthDistributionType");

        if(transactionLengthDistributionParameter == null)
            throw new DBException("requried transactionLengthDistributionParameter");

        if(transactionLengthDistributionType.compareTo("constant") == 0) {
            transactionLengthGenerator = new ConstantIntegerGenerator(Integer.parseInt(transactionLengthDistributionParameter));
        }
        else {
            throw new DBException("unimplemented transactionLengthDistribution type!");
        }

        client = new ThebesClient();
        try {
            client.open();
        } catch (Exception e) {
//            System.out.println(e);
//            e.printStackTrace();
            logger.warn("Error on init:", e);
            throw new DBException(e.getMessage());
        }
	}
	
	public void cleanup() throws DBException {
        try {
            client.commitTransaction();
        } catch(Exception e) {
            logger.warn("Error on cleanup:", e);
            throw new DBException(e.getMessage());
        }
        client.close();
	}

    public int beginTransaction() {
        try {
            client.beginTransaction();
        } catch (Exception e) {
            logger.warn("Error on insert:", e);
            return ERROR;
        }

        return OK;
    }

    public int endTransaction() {
        try {
            client.commitTransaction();
        } catch (Exception e) {
//            e.printStackTrace();
            logger.warn("Error on commit:", e);
            return ERROR;
        }

        return OK;
    }
	
	@Override
	public int delete(String table, String key) {
        try {
            client.put(key, null);
        } catch (Exception e) {
            logger.warn("Error on delete:", e);
            return ERROR;
        }
        return OK;
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            client.unsafe_load(key, ByteBuffer.wrap(values.values().iterator().next().toArray()));
        } catch (Exception e) {
            logger.warn("Error on insert:", e);
//            e.printStackTrace();
            return ERROR;
        }
		return OK;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
        try {
            ByteBuffer ret = client.get(key);



            if(ret == null || ret.array() == null) {
                if(fields != null)
                    result.put(fields.iterator().next(), new ByteArrayByteIterator("null".getBytes()));
                else
                    result.put("value", new ByteArrayByteIterator("null".getBytes()));
                return OK;
            }

            if(fields != null)
                result.put(fields.iterator().next(), new ByteArrayByteIterator(ret.array()));
            else
                result.put("value", new ByteArrayByteIterator(ret.array()));


        } catch (Exception e) {
            logger.warn("Error on read:", e);
//            e.printStackTrace();
            return ERROR;
        }
		return OK;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.warn("Thebes scans are not implemented!");
		return ERROR;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
        //update doesn't pass in the entire record, so we'd need to do read-modify-write
        try {
            client.put(key, ByteBuffer.wrap(values.values().iterator().next().toArray()));
        } catch (Exception e) {
            logger.warn("Error on update:", e);
//            e.printStackTrace();
            return ERROR;
        }
        return OK;
	}
	
	private int checkStore(String table) {
		return OK;
	}
}
