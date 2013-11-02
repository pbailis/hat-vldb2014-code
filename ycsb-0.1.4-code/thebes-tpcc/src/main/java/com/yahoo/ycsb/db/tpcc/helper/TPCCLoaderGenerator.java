
package com.yahoo.ycsb.db.tpcc.helper;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TSerializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.ycsb.db.tpcc.helper.random.RandomGenerator;
import com.yahoo.ycsb.db.tpcc.helper.rows.*;


public class TPCCLoaderGenerator {

    private RandomGenerator generator = new RandomGenerator(0);

	static List<String> lastNameStrings = Lists.newArrayList("BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING");
	
    private String generateZipCode() {
    	return generator.nstring(4, 4)+"11111";
    }
    
    private String generateDataString() {
    	String data = generator.astring(26, 50);
    	// should insert "ORIGINAL"
    	if(generator.nextDouble() < .1) {
    		int originalIndex = generator.number(0, data.length()-9);
    		data = data.substring(0, originalIndex)+"ORIGINAL"+data.substring(originalIndex+8);
    	}
    	
    	return data;
    }
    
    private String generateLastName(int customerNo) {
    	if(customerNo > 999)
    		return Integer.toString(generator.NURand(255, 0, 999));
    	else
    		return lastNameStrings.get(customerNo % 10) +
    			   lastNameStrings.get(customerNo/10 % 10) +
    			   lastNameStrings.get(customerNo/100 % 10);    	
    }
    
    ThreadLocal<TSerializer> serializer = new ThreadLocal<TSerializer>() {
        @Override
        protected TSerializer initialValue() {
            return new TSerializer();
        }
    };
    
    private ItemRow generateItemRow() {
    	return new ItemRow(// i_im_id
						  generator.number(0, 10000),
						  // i_name
						  generator.astring(14, 24),
						  // i_price
						  generator.fixedPoint(2, 1, 100),
						  // i_data,
						  generateDataString());
    }
        
    private WarehouseRow generateWarehouseRow() {
        return new WarehouseRow(// w_name
								generator.astring(6, 10),
								// w_street_1,
								generator.astring(6, 10),
								// w_street_2,
								generator.astring(6, 10),
								// w_city,
								generator.astring(10, 20),
								// w_state,
								generator.astring(2,2),
								// w_zip
								generateZipCode(),
								// w_tax
								generator.fixedPoint(4, 0, .2),
								// w_ytd
								300000.0);
        
    }
    
    private StockRow generateStockRow() {
    	return new StockRow(// s_quantity
							(short) generator.number(10, 100),
							// s_dist{1-10},
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							generator.astring(24, 24),
							// s_ytd,
							0,
							// s_order_cnt,
							0,
							// s_remote_cnt,
							0,
							// s_data
							generateDataString());
    								
    }
    
    private DistrictRow generateDistrictRow() {
    	return new DistrictRow(// d_name
    						   generator.astring(6, 10),
    						   // d_street_1
    						   generator.astring(10, 20),
    						   // d_street_2
    						   generator.astring(10, 20),
    						   // d_city
    						   generator.astring(10, 20),
    						   // d_state
    						   generator.astring(2, 2),
    						   // d_zip
    						   generateZipCode(),
    						   // d_tax
    						   generator.fixedPoint(4, 0, .2),
    						   30000);
    }
    
    private CustomerRow generateCustomerRow(int c_id) {
    	return new CustomerRow(// c_last
    						   generateLastName(c_id),
    						   // c_middle
    						   "OE",
    						   // c_first
    						   generator.astring(8, 16),
    						   // c_street_1
    						   generator.astring(10, 20),
    						   // c_street_2
    						   generator.astring(10, 20),
    						   // c_city
    						   generator.astring(10, 20),
    						   // c_state
    						   generator.astring(2, 2),
    						   // c_zip
    						   generateZipCode(),
    						   // c_phone
    						   generator.nstring(16, 16),
    						   // c_since
    						   Long.toString(System.currentTimeMillis()),
    						   // c_credit
    						   generator.nextDouble() < .1 ? "BC" : "GC",
    					 	   // c_credit_lim
    						   50000.0,
    						   // c_discount,
    						   generator.fixedPoint(4, 0, .5),
    						   // c_balance,
    						   -10.0,
    						   // c_ytd_payment
    						   10.0,
    						   // c_payment_cnt
    						   1,
    						   // c_delivery_cnt
    						   1,
    						   // c_data
    						   generator.astring(300, 500));
    }
    
    private HistoryRow generateHistoryRow() {
    	return new HistoryRow(// h_amount
    						  10.0,
    						  // h_data
    						  generator.astring(12, 24));
    }
    
    private OrderRow generateOrderRow(long o_id, int o_ol_count, int o_c_id, String date) {
    	return new OrderRow(// o_c_id
    					   o_c_id,
    					   // o_entry_d
    					   date,
    					   // o_carrier_id
    					   o_id < 2101 ? generator.number(1, 10) : -1,
    				       // o_ol_cnt
    					   o_ol_count,
    					   // o_all_local
    					   (short) 1);
    }
    
    private OrderLineRow generateOrderLineRow(long o_id, String o_date, long ol_number, int w_id) {   	
    	return new OrderLineRow(// ol_supply_w_id
    							w_id,
    							// ol_delivery_d
    							o_id < 2101 ? o_date : "",
    						    // ol_quantity
    							(short) 5,
    							// ol_amount
    							o_id < 2101 ? 0.0 : generator.fixedPoint(2, .01, 9999.99),
    							// ol_dist_info
    							generator.astring(24, 24));
    }
    
    private NewOrderRow generateNewOrderRow() {
    	return new NewOrderRow();
    }

    public Map<String, byte[]> getData() throws Exception {
        Map<String, byte[]> ret = Maps.newHashMap();

        for(int i_id = 1; i_id < 100001; ++i_id) {
        	ret.put(TPCCConstants.getItemKey(i_id),
        			serializer.get().serialize(generateItemRow()));
        }
        
        // warehouses
        for(int w_id = 1; w_id < 11; ++w_id) {
        	System.out.printf("Creating warehouse ID %d\n", w_id);
        	ret.put(TPCCConstants.getWarehouseKey(w_id),
        			serializer.get().serialize(generateWarehouseRow()));
        	
        	for(int s_i_id = 1; s_i_id < 100001; ++s_i_id) {
            	ret.put(TPCCConstants.getStockKey(w_id, s_i_id),
            			serializer.get().serialize(generateStockRow()));
        	}
        	
        	// districts
        	for(int d_id = 1; d_id < 11; ++d_id) {
            	System.out.printf("Creating district ID %d (warehouse: %d)\n", d_id, w_id);
            	ret.put(TPCCConstants.getDistrictKey(w_id, d_id),
            			serializer.get().serialize(generateDistrictRow()));
            
            	// customers
            	for(int c_id = 1; c_id < 3001; ++c_id) {
                	ret.put(TPCCConstants.getCustomerKey(w_id, d_id, c_id),
                			serializer.get().serialize(generateCustomerRow(c_id)));
                	
                	ret.put(TPCCConstants.getHistoryKey(w_id, d_id, c_id, System.currentTimeMillis()),
                			serializer.get().serialize(generateHistoryRow()));
                }
            	
            	// scramble customer ids
            	List<Integer> c_id_ordered = Lists.newArrayList();
            	
            	for(int c_id = 1; c_id < 3001; ++c_id) {
            		c_id_ordered.add(c_id);
            	}
            	
            	List<Integer> c_id_random = Lists.newArrayList();
            	for(int i = 0; i < 3000; ++i) {
            		c_id_random.add(c_id_ordered.remove(generator.number(0, c_id_ordered.size()-1)));
            	}
            	
            	// orders
            	for(long o_id = 1; o_id < 3001; ++o_id) {
            		String orderDate = Long.toString(System.currentTimeMillis());
            		int orderCount = generator.number(5, 15);
            		
                	ret.put(TPCCConstants.getOrderKey(w_id, d_id, o_id),
                			serializer.get().serialize(generateOrderRow(o_id, orderCount, c_id_random.get((int) o_id-1), orderDate)));    
                	
                	for(int ol_number = 0; ol_number < orderCount; ++ol_number) {
	                	ret.put(TPCCConstants.getOrderLineKey(w_id, d_id, o_id, ol_number),
	                			serializer.get().serialize(generateOrderLineRow(o_id, orderDate, ol_number, w_id)));
                	}
                	
                	if(o_id > 2101)
                		ret.put(TPCCConstants.getNewOrderKey(w_id, d_id, o_id),
                				serializer.get().serialize(generateNewOrderRow()));
            	}
        	}
        }

        return ret;
    }
}
