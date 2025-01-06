#pragma once
#include "types.hpp"
#include <iostream>
// 结合模板化处理不同类型的哈希计算
struct key_hash
{
   template <typename T>
   size_t operator()(const T &key) const
   {
      size_t h = 0;
      // 遍历 key.get_values() 返回的每个值
      for (const auto &val : key.get_values())
      {
         // 如果 val 是 Varchar 类型，逐字符计算哈希
         if constexpr (std::is_same_v<decltype(val), Varchar<16>>)
         {
            for (size_t i = 0; i < 16; ++i)
            {
               h ^= std::hash<char>{}(val.data[i]) + 0x9e3779b9 + (h << 6) + (h >> 2); // 哈希合并
            }
         }
         else if constexpr (std::is_same_v<decltype(val), Integer>)
         {
            // 如果 val 是 Integer 类型，直接使用哈希
            h ^= std::hash<Integer>{}(val) + 0x9e3779b9 + (h << 6) + (h >> 2);
         }
      }
      return h;
   }
};

struct warehouse_t
{
   static constexpr int id = 0;
   struct Key
   {
      static constexpr int id = 0;
      Integer w_id;
      Key() { w_id = 0; }
      Key(Integer w) : w_id(w){};
      bool operator==(const Key &other) { return w_id == other.w_id; }
      bool operator>(const Key &other) const { return w_id > other.w_id; }
      bool operator>=(const Key &other) const { return w_id >= other.w_id; }
      bool operator<(const Key &other) const { return w_id < other.w_id; }
      void operator++(int)
      {
         w_id++;
      }
      Integer getWId() const { return w_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &order)
      {
         os << "w_id: " << order.w_id << std::endl;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {w_id};
      }
   };
   Varchar<10> w_name;
   Varchar<20> w_street_1;
   Varchar<20> w_street_2;
   Varchar<20> w_city;
   Varchar<2> w_state;
   Varchar<9> w_zip;
   Numeric w_tax;
   Numeric w_ytd;
   uint8_t padding[1024]; // fix for contention; could be solved with contention split from
                          // http://cidrdb.org/cidr2021/papers/cidr2021_paper21.pdf
};

struct district_t
{
   static constexpr int id = 1;
   struct Key
   {
      static constexpr int id = 1;
      Integer d_w_id;
      Integer d_id;
      Key()
      {
         d_w_id = 0;
         d_id = 0;
      }
      Key(Integer w, Integer d) : d_w_id(w), d_id(d){};
      Key(int oneDimensionalInt)
      {
         d_w_id = (oneDimensionalInt - 1) / 10;
         d_id = (oneDimensionalInt - 1) % 10;
      }
      bool operator==(const Key &other) { return (d_w_id == other.d_w_id) && (d_id == other.d_id); }
      bool operator>(const Key &other) const
      {
         if (d_w_id > other.d_w_id)
            return true;
         if (d_w_id < other.d_w_id)
            return false;
         return (d_id > other.d_id); // equal
      }

      bool operator<(const Key &other) const
      {
         if (d_w_id < other.d_w_id)
            return true;
         if (d_w_id > other.d_w_id)
            return false;
         return (d_id < other.d_id); // equal
      }
      bool operator>=(const Key &other) const { return !(*this < other); }
      void operator++(int)
      {
         d_id++;
      }
      Integer getWId() const { return d_w_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &district)
      {
         os << "d_w_id: " << district.d_w_id << "d_id: " << district.d_id << std::endl;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {d_w_id, d_w_id};
      }
   };
   Varchar<10> d_name;
   Varchar<20> d_street_1;
   Varchar<20> d_street_2;
   Varchar<20> d_city;
   Varchar<2> d_state;
   Varchar<9> d_zip;
   Numeric d_tax;
   Numeric d_ytd;
   Integer d_next_o_id;
};

struct customer_t
{
   static constexpr int id = 2;
   struct Key
   {
      static constexpr int id = 2;
      Integer c_w_id;
      Integer c_d_id;
      Integer c_id;
      Key() = default;
      Key(Integer w, Integer d, Integer c) : c_w_id(w), c_d_id(d), c_id(c) {}
      Key(int oneDimensionalInt)
      {
         c_w_id = (oneDimensionalInt - 1) / 1'000'000;
         c_d_id = ((oneDimensionalInt - 1) % 1'000'000 + 1) / 10000;
         c_id = (oneDimensionalInt - 1) % 10000 + 1;
      }
      bool operator==(const Key &other) { return (c_w_id == other.c_w_id) && (c_d_id == other.c_d_id) && (c_id == other.c_id); }

      bool operator<(const Key &other) const
      {
         if (c_w_id < other.c_w_id)
            return true;
         if (c_w_id > other.c_w_id)
            return false;
         // equal c_w_id
         if (c_d_id < other.c_d_id)
            return true;
         if (c_d_id > other.c_d_id)
            return false;
         // equal
         return (c_id < other.c_id); // equal
      }
      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      void operator++(int)
      {
         c_id++;
      }
      Integer getWId() const { return c_w_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &customer)
      {
         os << "c_w_id: " << customer.c_w_id << " c_d_id: " << customer.c_d_id << " c_id: " << customer.c_id << std::endl;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {c_w_id, c_d_id, c_id};
      }
   };
   Varchar<16> c_first;
   Varchar<2> c_middle;
   Varchar<16> c_last;
   Varchar<20> c_street_1;
   Varchar<20> c_street_2;
   Varchar<20> c_city;
   Varchar<2> c_state;
   Varchar<9> c_zip;
   Varchar<16> c_phone;
   Timestamp c_since;
   Varchar<2> c_credit;
   Numeric c_credit_lim;
   Numeric c_discount;
   Numeric c_balance;
   Numeric c_ytd_payment;
   Numeric c_payment_cnt;
   Numeric c_delivery_cnt;
   Varchar<500> c_data;
};

struct customer_wdl_t
{
   static constexpr int id = 3;
   struct Key
   {
      static constexpr int id = 3;
      Integer c_w_id;
      Integer c_d_id;
      Varchar<16> c_last;
      Varchar<16> c_first;

      bool operator==(const Key &other) const
      {
         return (c_w_id == other.c_w_id) && (c_d_id == other.c_d_id) && (c_last == other.c_last) && (c_first == other.c_first);
      }

      bool operator<(const Key &other) const
      {
         if (c_w_id < other.c_w_id)
            return true;
         if (c_w_id > other.c_w_id)
            return false;

         if (c_d_id < other.c_d_id)
            return true;
         if (c_d_id > other.c_d_id)
            return false;

         if (c_last < other.c_last)
            return true;
         if (c_last > other.c_last)
            return false;

         return (c_first < other.c_first);
      }
      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      Integer getWId() const { return c_w_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &order)
      {
         os << order.c_d_id;
         return os;
      }
      // 获取所有值，用于哈希
      std::vector<Integer> get_values() const
      {
         std::vector<Integer> values = {c_w_id, c_d_id};
         // Varchar 类型的字段需要转换成相应的整数表示
         for (size_t i = 0; i < 16; ++i)
         {
            values.push_back(static_cast<Integer>(c_last.data[i]));
            values.push_back(static_cast<Integer>(c_first.data[i]));
         }
         return values;
      }
   };
   Integer c_id;
};

struct history_t
{
   static constexpr int id = 4;
   struct Key
   {
      static constexpr int id = 4;
      Integer thread_id;
      Integer h_pk;

      bool operator==(const Key &other) const
      {
         return (thread_id == other.thread_id) && (h_pk == other.h_pk);
      }

      bool operator<(const Key &other) const
      {
         if (thread_id < other.thread_id)
            return true;
         if (thread_id > other.thread_id)
            return false;
         return (h_pk < other.h_pk);
      }

      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      friend std::ostream &operator<<(std::ostream &os, const Key &order)
      {
         os << order.h_pk;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {thread_id, h_pk};
      }
   };
   Integer h_c_id;
   Integer h_c_d_id;
   Integer h_c_w_id;
   Integer h_d_id;
   Integer h_w_id;
   Timestamp h_date;
   Numeric h_amount;
   Varchar<24> h_data;
};

struct neworder_t
{
   static constexpr int id = 5;
   struct Key
   {
      static constexpr int id = 5;
      Integer no_w_id;
      Integer no_d_id;
      Integer no_o_id;
      Key() = default;
      Key(Integer w, Integer d, Integer o) : no_w_id(w), no_d_id(d), no_o_id(o){};
      Key(int oneDimensionalInt)
      {
         no_w_id = (oneDimensionalInt - 1) / 1'000'000;
         no_d_id = ((oneDimensionalInt - 1) % 1'000'000 + 1) / 10000;
         no_o_id = (oneDimensionalInt - 1) % 10000 + 1;
      }
      bool operator==(const Key &other) const
      {
         return (no_w_id == other.no_w_id) && (no_d_id == other.no_d_id) && (no_o_id == other.no_o_id);
      }

      bool operator<(const Key &other) const
      {
         if (no_w_id < other.no_w_id)
            return true;
         if (no_w_id > other.no_w_id)
            return false;
         // equal c_w_id
         if (no_d_id < other.no_d_id)
            return true;
         if (no_d_id > other.no_d_id)
            return false;
         // equal
         return (no_o_id < other.no_o_id); // equal
      }

      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      void operator++(int)
      {
         no_o_id++;
      }
      Integer getWId() const { return no_w_id; }
      // friend std::ostream &operator<<(std::ostream &os, const Key &order)
      // {
      //    os << order.no_o_id;
      //    return os;
      // }
      friend std::ostream &operator<<(std::ostream &os, const Key &neworder)
      {
         os << "no_w_id: " << neworder.no_w_id << " no_d_id: " << neworder.no_d_id << " no_o_id: " << neworder.no_o_id << std::endl;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {no_w_id, no_d_id, no_o_id};
      }
   };
};

struct order_t
{
   static constexpr int id = 6;
   struct Key
   {
      static constexpr int id = 6;
      Integer o_w_id;
      Integer o_d_id;
      Integer o_id;
      Key() = default;
      Key(Integer w, Integer d, Integer o) : o_w_id(w), o_d_id(d), o_id(o){};
      Key(int oneDimensionalInt)
      {
         o_w_id = (oneDimensionalInt - 1) / 1'000'000;
         o_d_id = ((oneDimensionalInt - 1) % 1'000'000 + 1) / 10000;
         o_id = (oneDimensionalInt - 1) % 10000 + 1;
      }
      bool operator==(const Key &other) const { return (o_w_id == other.o_w_id) && (o_d_id == other.o_d_id) && (o_id == other.o_id); }

      bool operator<(const Key &other) const
      {
         if (o_w_id < other.o_w_id)
            return true;
         if (o_w_id > other.o_w_id)
            return false;
         // equal c_w_id
         if (o_d_id < other.o_d_id)
            return true;
         if (o_d_id > other.o_d_id)
            return false;
         // equal
         return (o_id < other.o_id); // equal
      }

      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      void operator++(int)
      {
         o_id++;
      }
      Integer getWId() const { return o_w_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &order)
      {
         os << "o_w_id: " << order.o_w_id << " o_d_id: " << order.o_d_id << " o_id: " << order.o_id << std::endl;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {o_w_id, o_d_id, o_id};
      }
   };
   Integer o_c_id;
   Timestamp o_entry_d;
   Integer o_carrier_id;
   Numeric o_ol_cnt;
   Numeric o_all_local;
};

struct order_wdc_t
{
   static constexpr int id = 7;
   struct Key
   {
      static constexpr int id = 7;
      Integer o_w_id;
      Integer o_d_id;
      Integer o_c_id;
      Integer o_id;
      bool operator==(const Key &other) const
      {
         return (o_w_id == other.o_w_id) && (o_d_id == other.o_d_id) && (o_c_id == other.o_c_id) && (o_id == other.o_id);
      }

      bool operator<(const Key &other) const
      {
         if (o_w_id < other.o_w_id)
            return true;
         if (o_w_id > other.o_w_id)
            return false;

         if (o_d_id < other.o_d_id)
            return true;
         if (o_d_id > other.o_d_id)
            return false;

         if (o_c_id < other.o_c_id)
            return true;
         if (o_c_id > other.o_c_id)
            return false;
         return (o_id < other.o_id);
      }
      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      void operator++(int)
      {
         o_id++;
      }
      Integer getWId() const { return o_w_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &item)
      {
         os << item.o_id;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {o_w_id, o_d_id, o_c_id, o_id};
      }
   };
};

struct orderline_t
{
   static constexpr int id = 8;
   struct Key
   {
      static constexpr int id = 8;
      Integer ol_w_id;
      Integer ol_d_id;
      Integer ol_o_id;
      Integer ol_number;

      Key() = default;
      Key(Integer w, Integer d, Integer o, Integer num) : ol_w_id(w), ol_d_id(d), ol_o_id(o), ol_number(num){};
      Key(int oneDimensionalInt)
      {
         ol_w_id = (oneDimensionalInt - 1) / 5'000'000;
         ol_d_id = ((oneDimensionalInt - 1) % 5'000'000 + 1) / 300000;
         ol_o_id = ((oneDimensionalInt - 1) % 300000 + 1) / 30;
         ol_number = (oneDimensionalInt - 1) % 30 + 1;
      }

      bool operator==(const Key &other) const
      {
         return (ol_w_id == other.ol_w_id) && (ol_d_id == other.ol_d_id) && (ol_o_id == other.ol_o_id) && (ol_number == other.ol_number);
      }

      bool operator<(const Key &other) const
      {
         if (ol_w_id < other.ol_w_id)
            return true;
         if (ol_w_id > other.ol_w_id)
            return false;

         if (ol_d_id < other.ol_d_id)
            return true;
         if (ol_d_id > other.ol_d_id)
            return false;

         if (ol_o_id < other.ol_o_id)
            return true;
         if (ol_o_id > other.ol_o_id)
            return false;
         return (ol_number < other.ol_number);
      }
      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      void operator++(int)
      {
         ol_o_id++;
      }
      Integer getWId() const { return ol_w_id; }
      // friend std::ostream &operator<<(std::ostream &os, const Key &item)
      // {
      //    os << item.ol_d_id;
      //    return os;
      // }
      friend std::ostream &operator<<(std::ostream &os, const Key &orderline)
      {
         os << "ol_w_id: " << orderline.ol_w_id << " ol_d_id: " << orderline.ol_d_id
            << " ol_o_id: " << orderline.ol_o_id << " ol_number: " << orderline.ol_number << std::endl;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {ol_w_id, ol_d_id, ol_o_id, ol_number};
      }
   };
   Integer ol_i_id;
   Integer ol_supply_w_id;
   Timestamp ol_delivery_d;
   Numeric ol_quantity;
   Numeric ol_amount;
   Varchar<24> ol_dist_info;
};

struct item_t
{
   static constexpr int id = 9;
   struct Key
   {
      static constexpr int id = 9;
      Integer i_id;

      bool operator==(const Key &other) { return i_id == other.i_id; }
      bool operator>(const Key &other) const { return i_id > other.i_id; }
      bool operator>=(const Key &other) const { return i_id >= other.i_id; }
      bool operator<(const Key &other) const { return i_id < other.i_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &item)
      {
         os << item.i_id;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {i_id};
      }
   };
   Integer i_im_id;
   Varchar<24> i_name;
   Numeric i_price;
   Varchar<50> i_data;
};

struct stock_t
{
   static constexpr int id = 10;
   struct Key
   {
      static constexpr int id = 10;
      Integer s_w_id;
      Integer s_i_id;
      Key() = default;
      Key(Integer w, Integer i) : s_w_id(w), s_i_id(i){};
      Key(int oneDimensionalInt)
      {
         s_w_id = (oneDimensionalInt - 1) / 1'000'000;
         s_i_id = (oneDimensionalInt - 1) % 1'000'000 + 1;
      }
      bool operator==(const Key &other) const
      {
         return (s_w_id == other.s_w_id) && (s_i_id == other.s_i_id);
      }

      bool operator<(const Key &other) const
      {
         if (s_w_id < other.s_w_id)
            return true;
         if (s_w_id > other.s_w_id)
            return false;
         return (s_i_id < other.s_i_id);
      }
      bool operator>(const Key &other) const { return (other < *this); }
      bool operator>=(const Key &other) const { return !(*this < other); }
      void operator++(int)
      {
         s_i_id++;
      }
      Integer getWId() const { return s_w_id; }
      friend std::ostream &operator<<(std::ostream &os, const Key &stock)
      {
         os << "s_w_id: " << stock.s_w_id << "s_i_id: " << stock.s_i_id << std::endl;
         return os;
      }
      std::vector<Integer> get_values() const
      {
         return {s_w_id, s_i_id};
      }
   };
   Numeric s_quantity;
   Varchar<24> s_dist_01;
   Varchar<24> s_dist_02;
   Varchar<24> s_dist_03;
   Varchar<24> s_dist_04;
   Varchar<24> s_dist_05;
   Varchar<24> s_dist_06;
   Varchar<24> s_dist_07;
   Varchar<24> s_dist_08;
   Varchar<24> s_dist_09;
   Varchar<24> s_dist_10;
   Numeric s_ytd;
   Numeric s_order_cnt;
   Numeric s_remote_cnt;
   Varchar<50> s_data;
};