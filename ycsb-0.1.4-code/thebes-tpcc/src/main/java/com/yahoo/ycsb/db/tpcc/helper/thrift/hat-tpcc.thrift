#!/usr/local/bin/thrift --gen java

namespace java com.yahoo.ycsb.db.tpcc.helper.rows

# pkey = i_id
struct ItemRow {
  1: i32 i_im_id,
  2: string i_name,
  3: double i_price,
  4: string i_data
}

#pkey w_id
struct WarehouseRow {
  1: string w_name,
  2: string w_street_1,
  3: string w_street_2,
  4: string w_city,
  5: string w_state,
  6: string w_zip,
  7: double w_tax,
  8: double w_ytd
}

#pkey w_id:i_id:s_i_id
struct StockRow {
  3: i16 s_quantity,
  4: string s_dist_01,
  5: string s_dist_02,
  6: string s_dist_03,
  7: string s_dist_04,
  8: string s_dist_05,
  9: string s_dist_06,
  10: string s_dist_07,
  11: string s_dist_08,
  12: string s_dist_09,
  13: string s_dist_10,
  14: i32 s_ytd,
  15: i32 s_order_cnt,
  16: i32 s_remote_cnt,
  17: string s_data
}

#pkey w_id:d_id
struct DistrictRow {
  1: string d_name,
  2: string d_street_1,
  3: string d_street_2,
  4: string d_city,
  5: string d_state,
  6: string d_zip,
  7: double d_tax,
  8: double d_ytd
}

#pkey w_id:d_id:c_id
struct CustomerRow {
    1: string c_last,
    2: string c_middle,
    3: string c_first,
    5: string c_street_1,
    6: string c_street_2,
    7: string c_city,
    8: string c_state,
    9: string c_zip,
    10: string c_phone,
    11: string c_since,
    12: string c_credit,
    13: double c_credit_lim,
    14: double c_discount,
    15: double c_balance,
    16: double c_ytd_payment,
    17: i32 c_payment_cnt,
    18: i32 c_delivery_cnt,
    19: string c_data
}

#pkey w_id:d_id:c_id:date
struct HistoryRow {
    1: double h_amount,
    2: string h_data
}

#pkey w_id:d_id:o_id
struct OrderRow {
    1: i32 o_c_id,
    2: string o_entry_d,
    3: i32 o_carrier_id,
    4: i32 o_ol_cnt,
    5: i16 o_all_local
}

#pkey w_id:d_id:o_id:0l_number
struct OrderLineRow {
    3: i32 ol_supply_w_id,
    4: string ol_delivery_d,
    5: i16 ol_quantity,
    6: double ol_amount,
    7: string ol_dist_info
}

#pkey w_id:d_id:o_id
struct NewOrderRow {
    1: optional string data
}