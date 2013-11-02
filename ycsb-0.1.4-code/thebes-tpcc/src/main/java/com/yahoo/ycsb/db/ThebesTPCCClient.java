package com.yahoo.ycsb.db;

import java.lang.Exception;
import java.lang.System;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

import com.google.common.collect.Lists;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.TPCCDB;
import com.yahoo.ycsb.db.tpcc.helper.TPCCConstants;
import com.yahoo.ycsb.db.tpcc.helper.TPCCLoaderGenerator;
import com.yahoo.ycsb.db.tpcc.helper.random.RandomGenerator;
import com.yahoo.ycsb.db.tpcc.helper.rows.CustomerRow;
import com.yahoo.ycsb.db.tpcc.helper.rows.DistrictRow;
import com.yahoo.ycsb.db.tpcc.helper.rows.ItemRow;
import com.yahoo.ycsb.db.tpcc.helper.rows.NewOrderRow;
import com.yahoo.ycsb.db.tpcc.helper.rows.OrderLineRow;
import com.yahoo.ycsb.db.tpcc.helper.rows.OrderRow;
import com.yahoo.ycsb.db.tpcc.helper.rows.StockRow;
import com.yahoo.ycsb.db.tpcc.helper.rows.WarehouseRow;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import edu.berkeley.thebes.client.ThebesClient;

public class ThebesTPCCClient extends DB implements TPCCDB {

    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;
	
	Measurements _measurements = Measurements.getMeasurements();
    
    ThebesClient client;
    
    private RandomGenerator generator = new RandomGenerator((int) (System.currentTimeMillis() % Integer.MAX_VALUE));
    
    private final int HOME_WAREHOUSE = generator.number(1, 10);
    
    TSerializer serializer = new TSerializer();
    TDeserializer deserializer = new TDeserializer();


    private final Logger logger = Logger.getLogger(ThebesTPCCClient.class);

    public void load() throws Exception {
    	TPCCLoaderGenerator generator = new TPCCLoaderGenerator();
    	
    	Map<String, byte[]> toLoad = generator.getData();
    	
    	long keyNo = 0;
    	final long totalKeys = toLoad.size();
    	
    	for(String key : toLoad.keySet()) {
    		client.beginTransaction();
    		client.put(key,  ByteBuffer.wrap(toLoad.get(key)));
    		client.commitTransaction();
    		
    		System.out.printf("%d/%d\n", keyNo++, totalKeys);
    	}
    }
    
	public void init() throws DBException {
        client = new ThebesClient();
        try {
            client.open();
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            throw new DBException(e.getMessage());
        }
	}
	
	public void cleanup() throws DBException {
        try {
            client.commitTransaction();
        } catch(Exception e) {
        	System.err.println(e.getMessage());
        	e.printStackTrace();
            throw new DBException(e.getMessage());
        }
        client.close();
	}
	
    private int runNewOrder() {
		long st=System.nanoTime();
        try {
            client.beginTransaction();
            
            long O_ID = generator.nextLong();
            
            // should technically be [1, 10]
            int D_ID = generator.number(1, 10);
            int C_ID = generator.NURand(1023,1,3000);
            int OL_CNT = generator.number(5, 15);
            boolean rollback = generator.nextDouble() < .01;
            
            List<Integer> warehouseIDs = Lists.newArrayList();
            
            boolean allLocal = true;
            
            for(int i = 0; i < OL_CNT; ++i) {
            	int nextId = generator.nextDouble() > .01 ? HOME_WAREHOUSE : generator.numberExcluding(1, 10, HOME_WAREHOUSE);
            	warehouseIDs.add(nextId);
            	allLocal &= nextId == HOME_WAREHOUSE;
            }
            
        	String O_ENTRY_D = Long.toString(System.currentTimeMillis());
        	
        	WarehouseRow warehouse = new WarehouseRow();
        	byte[] warehouseByteRet = client.get(TPCCConstants.getWarehouseKey(HOME_WAREHOUSE)).array();
        	deserializer.deserialize(warehouse, warehouseByteRet);
        	
        	double warehouseTaxRate = warehouse.getW_tax();
        	
        	DistrictRow district = new DistrictRow();
        	byte[] districtByteRet = client.get(TPCCConstants.getDistrictKey(HOME_WAREHOUSE, D_ID)).array();
        	deserializer.deserialize(district, districtByteRet);
        	
        	double districtTaxRate = district.getD_tax();
        	
        	CustomerRow customer = new CustomerRow();
        	byte[] customerByteRet = client.get(TPCCConstants.getCustomerKey(HOME_WAREHOUSE, D_ID, C_ID)).array();
        	deserializer.deserialize(customer, customerByteRet);
            
        	OrderRow order = new OrderRow(C_ID,
        								  O_ENTRY_D,
        								  // O_CARRIER_ID
        								  -1,
        								  OL_CNT,
        								  allLocal ? (short) 0 : (short) 1);
        	client.put(TPCCConstants.getOrderKey(HOME_WAREHOUSE, D_ID, O_ID), ByteBuffer.wrap(serializer.serialize(order)));
        	
        	NewOrderRow newOrder = new NewOrderRow();
        	client.put(TPCCConstants.getNewOrderKey(HOME_WAREHOUSE, D_ID, O_ID), ByteBuffer.wrap(serializer.serialize(newOrder)));
        	
        	double totalAmount = 0;
        	
            for(int ol_cnt = 0; ol_cnt < OL_CNT; ++ol_cnt) {
            	int OL_I_ID = generator.NURand(8191,1,100000);
            	int OL_QUANTITY = generator.number(1, 10);
            	
            	if(rollback && ol_cnt == OL_CNT-1) {
            		OL_I_ID = -1;
            	}
            	
            	ItemRow item = new ItemRow();
            	ByteBuffer itemBytebufferRet = client.get(TPCCConstants.getItemKey(OL_I_ID));
            	if(itemBytebufferRet == null) {
            		client.abortTransaction();
            		return OK;
            	}
            	deserializer.deserialize(item, itemBytebufferRet.array());
            	
            	int OL_SUPPLY_W_ID = warehouseIDs.get(ol_cnt);
            	
            	StockRow stock = new StockRow();
            	byte[] stockByteRet = client.get(TPCCConstants.getStockKey(OL_SUPPLY_W_ID, OL_I_ID)).array();
            	deserializer.deserialize(stock, stockByteRet);
            	
            	int currentQuantity = stock.getS_quantity();
            	if(currentQuantity > OL_QUANTITY+10)
            		currentQuantity -= OL_QUANTITY;
            	else
            		currentQuantity = currentQuantity - OL_QUANTITY + 91;
            	
            	stock.setS_quantity((short) currentQuantity);
            	stock.setS_ytd(stock.getS_ytd()+OL_QUANTITY);
            	stock.setS_order_cnt(stock.getS_order_cnt()+1);
            	if(OL_SUPPLY_W_ID != HOME_WAREHOUSE)
            		stock.setS_remote_cnt(stock.getS_remote_cnt()+1);
            	
            	client.put(TPCCConstants.getStockKey(OL_SUPPLY_W_ID, OL_I_ID), ByteBuffer.wrap(serializer.serialize(stock)));
            	
            	double OL_AMOUNT = OL_QUANTITY*item.getI_price();
            	totalAmount += OL_AMOUNT;
            	
            	// not sure why this matters, but let's be compliant
            	String brandGeneric = "B";            	
            	if(item.getI_data().contains("ORIGINAL") && stock.getS_data().contains("ORIGINAL"))
            		brandGeneric = "S";
            	
            	String districtData;
            	
            	if(D_ID == 1)
            		districtData = stock.getS_dist_01();
            	else if(D_ID == 2)
            		districtData = stock.getS_dist_02();
            	else if(D_ID == 3)
            		districtData = stock.getS_dist_03();
            	else if(D_ID == 4)
            		districtData = stock.getS_dist_04();
            	else if(D_ID == 5)
            		districtData = stock.getS_dist_05();
            	else if(D_ID == 6)
            		districtData = stock.getS_dist_06();
            	else if(D_ID == 7)
            		districtData = stock.getS_dist_07();
            	else if(D_ID == 8)
            		districtData = stock.getS_dist_08();
            	else if(D_ID == 9)
            		districtData = stock.getS_dist_09();
            	else
            		districtData = stock.getS_dist_10();

            	
            	OrderLineRow orderLine = new OrderLineRow(OL_SUPPLY_W_ID,
            											  "",
            											  (short) OL_QUANTITY,
            											  OL_AMOUNT,
            											  districtData);
            	client.put(TPCCConstants.getOrderLineKey(HOME_WAREHOUSE, D_ID, O_ID, ol_cnt), ByteBuffer.wrap(serializer.serialize(orderLine)));            			
            }
            
            totalAmount*=(1-customer.getC_discount())*(1+warehouse.getW_tax()+district.getD_tax());
            
            client.commitTransaction();
            
            
            return OK;
        } catch (Exception e) {
            System.out.println("EXCEPTION IN NEWORDER: "+e.getMessage());
            e.printStackTrace();
            return ERROR;
        } finally {
    		long en=System.nanoTime();
    		_measurements.measure("TRANSACTION",(int)((en-st)/1000));
        }
    }
	
	@Override
	public int delete(String table, String key) {
        return runNewOrder();
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return runNewOrder();
	}

	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result) {
        return runNewOrder();
    }

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return runNewOrder();
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return runNewOrder();
	}
	
	private int checkStore(String table) {
		return OK;
	}
}
