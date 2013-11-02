package com.yahoo.ycsb.db.tpcc.helper;

public class TPCCConstants {
	public static String getItemKey(int i_id) {
		return String.format("ITEM:%d", i_id);
	}
	
	public static String getWarehouseKey(int w_id) {
		return String.format("WARE:%d", w_id);
	}

	public static String getStockKey(int w_id, int s_i_id) {
		return String.format("STOCK:W:%d:S:%d", w_id, s_i_id);
	}
	
	public static String getDistrictKey(int w_id, int d_id) {
		return String.format("DIST:W:%d:D:%d", w_id, d_id);
	}
	
	public static String getCustomerKey(int w_id, int d_id, int c_id) {
		return String.format("CUST:W:%d:D:%d:C:%d", w_id, d_id, c_id);
	}
	
	public static String getHistoryKey(int w_id, int d_id, int c_id, long date) {
		return String.format("HIST:W:%d:D:%d:C:%d:D:%d", w_id, d_id, c_id, date);
	}
	
	public static String getOrderKey(int w_id, int d_id, long o_id) {
		return String.format("ORD:W:%d:D:%d:O:%d", w_id, d_id, o_id);
	}
	
	public static String getOrderLineKey(int w_id, int d_id, long o_id, int ol_number) {
		return String.format("L_ORD:W:%d:D:%d:O:%d:OL:%d", w_id, d_id, o_id, ol_number);
	}
	
	public static String getNewOrderKey(int w_id, int d_id, long o_id) {
		return String.format("N_ORD:W:%d:D:%d:O:%d", w_id, d_id, o_id);
	}
}
