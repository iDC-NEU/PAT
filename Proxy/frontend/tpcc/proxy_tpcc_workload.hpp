#include "proxy_types.hpp"
#include <iostream>
#include "../backend/Proxy/utils/RandomGenerator.hpp"
#include <iomanip>

static constexpr uint64_t MAX_THREADS = 100;
thread_local Integer thread_id = 0;
Integer variable_for_workload[MAX_THREADS];     // max threads 40
thread_local Integer total_new_order = 0;       // tpc-c h_id
thread_local Integer remote_new_order = 0;      // tpc-c h_id as for specification
thread_local Integer remote_node_new_order = 0; // tpc-c h_id
thread_local Integer delivery_aborts = 0;       // tpc-c h_id

enum transaction_types
{
   NEW_ORDER = 0,
   DELIVERY = 1,
   STOCK_LEVEL = 2,
   ORDER_STATUS = 3,
   PAYMENT = 4,
   MAX,
};

thread_local std::vector<uint64_t> txns(transaction_types::MAX);
thread_local std::vector<uint64_t> txn_latencies(transaction_types::MAX);
thread_local std::vector<uint64_t> txn_paymentbyname_latencies(10);

struct Partition
{
   uint64_t begin;
   uint64_t end;
};

Partition warehouse_range_node;
// load
Integer warehouseCount;
// -------------------------------------------------------------------------------------
static constexpr Integer OL_I_ID_C = 7911; // in range [0, 8191]
static constexpr Integer C_ID_C = 259;     // in range [0, 1023]
// NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
// be within [65, 119]
static constexpr Integer C_LAST_LOAD_C = 157; // in range [0, 255]
static constexpr Integer C_LAST_RUN_C = 223;  // in range [0, 255]
// -------------------------------------------------------------------------------------
static constexpr Integer ITEMS_NO = 100000; // 100K

// [0, n)
Integer rnd(Integer n)
{
   return Proxy::utils::RandomGenerator::getRand(0, n);
}

// [low, high]
Integer urand(Integer low, Integer high)
{
   return rnd(high - low + 1) + low;
}

Integer urandexcept(Integer low, Integer high, Integer v)
{
   if (high <= low)
      return low;
   Integer r = rnd(high - low) + low;
   if (r >= v)
      return r + 1;
   else
      return r;
}

template <int maxLength>
Varchar<maxLength> randomastring(Integer minLenStr, Integer maxLenStr)
{
   assert(maxLenStr <= maxLength);
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<maxLength> result;
   for (Integer index = 0; index < len; index++)
   {
      Integer i = rnd(62);
      if (i < 10)
         result.append(48 + i);
      else if (i < 36)
         result.append(64 - 10 + i);
      else
         result.append(96 - 36 + i);
   }
   return result;
}

Varchar<16> randomnstring(Integer minLenStr, Integer maxLenStr)
{
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<16> result;
   for (Integer i = 0; i < len; i++)
      result.append(48 + rnd(10));
   return result;
}

Varchar<16> namePart(Integer id)
{
   assert(id < 10);
   Varchar<16> data[] = {"Bar", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
   return data[id];
}

Varchar<16> genName(Integer id)
{
   return namePart((id / 100) % 10) || namePart((id / 10) % 10) || namePart(id % 10);
}

Numeric randomNumeric(Numeric min, Numeric max)
{
   double range = (max - min);
   double div = RAND_MAX / range;
   return min + (Proxy::utils::RandomGenerator::getRandU64() / div);
}

Varchar<9> randomzip()
{
   Integer id = rnd(10000);
   Varchar<9> result;
   result.append(48 + (id / 1000));
   result.append(48 + (id / 100) % 10);
   result.append(48 + (id / 10) % 10);
   result.append(48 + (id % 10));
   return result || Varchar<9>("11111");
}

Integer nurand(Integer a, Integer x, Integer y, Integer C = 42)
{
   // TPC-C random is [a,b] inclusive
   // in standard: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
   // return (((rnd(a + 1) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
   return (((urand(0, a) | urand(x, y)) + C) % (y - x + 1)) + x;
   // incorrect: return (((rnd(a) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
}

inline Integer getItemID()
{
   // OL_I_ID_C
   return nurand(8191, 1, ITEMS_NO, OL_I_ID_C);
}
inline Integer getCustomerID()
{
   // C_ID_C
   return nurand(1023, 1, 3000, C_ID_C);
   // return urand(1, 3000);
}
inline Integer getNonUniformRandomLastNameForRun()
{
   // C_LAST_RUN_C
   return nurand(255, 0, 999, C_LAST_RUN_C);
}
inline Integer getNonUniformRandomLastNameForLoad()
{
   // C_LAST_LOAD_C
   return nurand(255, 0, 999, C_LAST_LOAD_C);
}

std::string convertIntVectorToString(const std::vector<Integer> &intVector)
{
   std::string str = "{";
   for (size_t i = 0; i < intVector.size(); ++i)
   {
      if (i == 0)
      {
         str += std::to_string(intVector[i]);
      }
      else
      {
         str += ";" + std::to_string(intVector[i]);
      }
   }
   return str + "}";
}

Timestamp currentTimestamp()
{
   return 1;
}

// -------------------------------------------------------------------------------------
std::string newOrderRndCreate(Integer w_id)
{
   // -------------------------------------------------------------------------------------
   total_new_order++;
   // -------------------------------------------------------------------------------------
   Integer d_id = urand(1, 10);
   Integer c_id = getCustomerID();
   Integer ol_cnt = urand(5, 15);

   std::vector<Integer> lineNumbers;
   lineNumbers.reserve(15);
   std::vector<Integer> supwares;
   supwares.reserve(15);
   std::vector<Integer> itemids;
   itemids.reserve(15);
   std::vector<Integer> qtys;
   qtys.reserve(15);
   for (Integer i = 1; i <= ol_cnt; i++)
   {
      Integer supware = w_id;

      if (FLAGS_distribution && urand(1, 100) <= float(FLAGS_distribution_rate)/10)
      { // remote transaction
        supware = urandexcept(1, warehouseCount, w_id);
        remote_new_order++;
      }

      Integer itemid = getItemID();
      if (false && (i == ol_cnt) && (urand(1, 100) == 1)) // invalid item => random
         itemid = 0;
      lineNumbers.push_back(i);
      supwares.push_back(supware);
      itemids.push_back(itemid);
      qtys.push_back(urand(1, 10));
   }
   // newOrder(w_id, d_id, c_id, lineNumbers, supwares, itemids, qtys, currentTimestamp());

   return "newOrder(" + std::to_string(w_id) + "," + std::to_string(d_id) + "," + std::to_string(c_id) + "," + convertIntVectorToString(lineNumbers) + "," + convertIntVectorToString(supwares) + "," + convertIntVectorToString(itemids) + "," + convertIntVectorToString(qtys) + "," + std::to_string(currentTimestamp()) + ")";
}

std::string stockLevelRndCreate(Integer w_id)
{
   return "stockLevel(" + std::to_string(w_id) + "," + std::to_string(urand(1, 10)) + "," + std::to_string(urand(10, 20)) + ")";
}

std::string orderStatusRndCreate(Integer w_id)
{
   Integer d_id = urand(1, 10);
   if (urand(1, 100) <= 40)
   {
      // orderStatusId(w_id, d_id, getCustomerID());
      return "orderStatusId(" + std::to_string(w_id) + "," + std::to_string(d_id) + "," + std::to_string(getCustomerID()) + ")";
   }
   else
   {
      // orderStatusName(w_id, d_id, genName(getNonUniformRandomLastNameForRun()));
      return "orderStatusName(" + std::to_string(w_id) + "," + std::to_string(d_id) + "," + genName(getNonUniformRandomLastNameForRun()).toString() + ")";
   }
}

/*
CREATE PROCEDURE paymentByName(
    IN w_id INT,
    IN d_id INT,
    IN c_w_id INT,
    IN c_d_id INT,
    IN c_last VARCHAR(16),
    IN h_date TIMESTAMP,
    IN h_amount NUMERIC,
    IN datetime TIMESTAMP
)
BEGIN
    -- 1. 仓库更新
    UPDATE warehouse SET w_ytd = w_ytd + h_amount WHERE w_id = w_id;

    -- 2. 地区更新
    UPDATE district SET d_ytd = d_ytd + h_amount WHERE w_id = w_id AND d_id = d_id;

    -- 3. 顾客ID检索
    DECLARE c_id INT;
    SELECT c_id INTO c_id
    FROM customer_wdl
    WHERE c_w_id = c_w_id AND c_d_id = c_d_id AND c_last = c_last
    LIMIT 1;

    IF c_id IS NULL THEN
        -- 回滚或处理事务中止的逻辑
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Customer not found';
        -- 或者使用 ROLLBACK 语句进行回滚
        -- ROLLBACK;
    END IF;

    -- 4. 顾客信息更新
    UPDATE customer
    SET
        c_balance = c_balance - h_amount,
        c_ytd_payment = c_ytd_payment + h_amount,
        c_payment_cnt = c_payment_cnt + 1
    WHERE c_w_id = c_w_id AND c_d_id = c_d_id AND c_id = c_id;

    -- 5. 历史更新
    INSERT INTO history (t_id, h_id, c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data)
    VALUES (thread_id, variable_for_workload[thread_id]++, c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, CONCAT(w_name, '    ', d_name));

END;
*/
std::string deliveryRndCreate(Integer w_id)
{
   Integer carrier_id = urand(1, 10);
   // delivery(w_id, carrier_id, currentTimestamp());
   return "delivery(" + std::to_string(w_id) + "," + std::to_string(carrier_id) + "," + std::to_string(currentTimestamp()) + ")";
}

std::string doubleToString(double d, int precision = 15)
{
   std::stringstream ss;
   ss << std::setprecision(precision) << d;
   return ss.str();
}

std::string paymentRndCreate(Integer w_id)
{
   Integer d_id = urand(1, 10);
   Integer c_w_id = w_id;
   Integer c_d_id = d_id;

   if (FLAGS_distribution && urand(1, 100) < (float(FLAGS_distribution_rate)/2 * 3))
   {
      c_w_id = urandexcept(1, warehouseCount, w_id);
      c_d_id = urand(1, 10);
   }

   Numeric h_amount = randomNumeric(1.00, 5000.00);
   Timestamp h_date = currentTimestamp();

   if (urand(1, 100) <= 60)
   {
      // paymentByName(w_id, d_id, c_w_id, c_d_id, genName(getNonUniformRandomLastNameForRun()), h_date, h_amount, currentTimestamp());
      return "paymentByName(" + std::to_string(w_id) + "," + std::to_string(d_id) + "," + std::to_string(c_w_id) + "," + std::to_string(c_d_id) + "," + genName(getNonUniformRandomLastNameForRun()).toString() + "," + std::to_string(h_date) + "," + doubleToString(h_amount) + "," // 精度？
             + std::to_string(currentTimestamp()) + ")";
   }
   else
   {
      // paymentById(w_id, d_id, c_w_id, c_d_id, getCustomerID(), h_date, h_amount, currentTimestamp());
      return "paymentById(" + std::to_string(w_id) + "," + std::to_string(d_id) + "," + std::to_string(c_w_id) + "," + std::to_string(c_d_id) + "," + std::to_string(getCustomerID()) + "," + std::to_string(h_date) + "," + doubleToString(h_amount) + "," // 精度？
             + std::to_string(currentTimestamp()) + ")";
   }
}

int txn_next_id = 0;
std::string txCreate(Integer w_id)
{
   // micro-optimized version of weighted distribution
   // int rnd = Proxy::utils::RandomGenerator::getRand(0, 10000);
   std::string sql = "[" + std::to_string(++txn_next_id) + "]";
   // if (rnd < 4300)
   // {
   //    txns[transaction_types::PAYMENT]++;
   //    return sql + paymentRndCreate(w_id);
   //    ;
   // }
   // rnd -= 4300;
   // if (rnd < 400)
   // {
   //    txns[transaction_types::ORDER_STATUS]++;
   //    return sql + orderStatusRndCreate(w_id);
   // }
   // rnd -= 400;
   // if (rnd < 400)
   // {
   //    txns[transaction_types::DELIVERY]++;
   //    return sql + deliveryRndCreate(w_id);
   // }
   // rnd -= 400;
   // if (rnd < 400)
   // {
   //    return sql + stockLevelRndCreate(w_id);
   // }
   // rnd -= 400;
   txns[transaction_types::NEW_ORDER]++;
   return sql + newOrderRndCreate(w_id);
}