#ifndef K3_RUNTIME_BUILTINS_H
#define K3_RUNTIME_BUILTINS_H

#include <ctime>
#include <chrono>
#include <climits>
#include <fstream>
#include <sstream>
#include <string>
#include <climits>
#include <functional>

#include "re2/re2.h"
#include "strtk.hpp"

#include "BaseTypes.hpp"
#include "BaseString.hpp"
#include "Common.hpp"
#include "dataspace/Dataspace.hpp"


// Hashing:
namespace boost {

template<>
struct hash<boost::asio::ip::address> {
  size_t operator()(boost::asio::ip::address const& a) const {
    return hash_value(a.to_string());
  }
};

} // boost

namespace K3 {

  template <class C, class F>
  void read_records(std::istream& in, C& container, F read_record) {

    std::string tmp_buffer;
    while (!in.eof()) {
      container.insert(read_record(in, tmp_buffer));
      in >> std::ws;
    }

    return;
  }

  // Standard context for common builtins that use a handle to the engine (via inheritance)
  class __standard_context : public __k3_context {
    public:
    __standard_context(Engine& engine);

    unit_t openBuiltin(string_impl ch_id, string_impl builtin_ch_id, string_impl fmt);

    unit_t openFile(string_impl ch_id, string_impl path, string_impl fmt, string_impl mode);

    unit_t openSocket(string_impl ch_id, Address a, string_impl fmt, string_impl mode);

    unit_t close(string_impl chan_id);

    int random(int n);

    double randomFraction(unit_t);

    template <class T>
    int hash(const T& x) {
      // We implement hash_value for all of our types.
      // for ordered containers, so we may as well delegate to that.
      return static_cast<int>(hash_value(x));
    }

    template <class T>
    T range(int i) {
      T result;
      for (int j = 0; j < i; j++) {
        result.insert(j);
      }
      return result;
    }

    int truncate(double n) { return (int)n; }

    double real_of_int(int n) { return (double)n; }

    int get_max_int(unit_t) { return INT_MAX; }

    unit_t print(string_impl message);


    // TODO, implement, sharing code with prettify()
    template <class T>
    string_impl show(T t) {
      return string_impl("TODO: implement show()");
    }

    template <class T>
    T error(unit_t) {
      throw std::runtime_error("Error. Terminating");
      return *((T *) nullptr);
    }

    template <class T>
    unit_t ignore(T t) {
      return unit_t();
    }

    // TODO add a member to base_string, call that instead
    int strcomp(const string_impl& s1,const string_impl& s2) {
      const char* c1 = s1.c_str();
      const char* c2 = s2.c_str();
      if (c1 && c2) {
        return strcmp(c1,c2);
      }
      else if(c1) {
        return 1;
      }
      else if(c2) {
        return -1;
      }
      else {
        return 0;
      }

    }

    unit_t haltEngine(unit_t);

    unit_t drainEngine(unit_t);

    unit_t sleep(int n);

    template <template <class> class M, template <class> class C, template <typename ...> class R>
    unit_t loadGraph(string_impl filepath, M<R<int, C<R_elem<int>>>>& c) {
      std::string tmp_buffer;
      std::ifstream in(filepath);

      int source;
      std::size_t position;
      while (!in.eof()) {
	C<R_elem<int>> edge_list;

	std::size_t start = 0;
	std::size_t end = start;
	std::getline(in, tmp_buffer);

	end = tmp_buffer.find(",", start);
	source = std::atoi(tmp_buffer.substr(start, end - start).c_str());

	start = end + 1;

	while (end != std::string::npos) {
	  end = tmp_buffer.find(",", start);
	  edge_list.insert(R_elem<int>(std::atoi(tmp_buffer.substr(start, end - start).c_str())));
	  start = end + 1;
	}

	c.insert(R<int, C<R_elem<int>>> { source, std::move(edge_list)});
	in >> std::ws;
      }

      return unit_t {};
    }
    // TODO tpch loaders...move elsewhere
      unit_t lineitemLdr(string file,
      _Collection<R_l_comments_l_commitdate_l_discount_l_extendedprice_l_linenumber_l_linestatus_l_orderkey_l_partkey_l_quantity_l_receiptdate_l_returnflag_l_shipdate_l_shipinstruct_l_shipmode_l_suppkey_l_tax<K3::base_string,
      K3::base_string, double, double, int, K3::base_string, int, int, double, K3::base_string,
      K3::base_string, K3::base_string, K3::base_string, K3::base_string, int, double>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_l_comments_l_commitdate_l_discount_l_extendedprice_l_linenumber_l_linestatus_l_orderkey_l_partkey_l_quantity_l_receiptdate_l_returnflag_l_shipdate_l_shipinstruct_l_shipmode_l_suppkey_l_tax<K3::base_string,
          K3::base_string, double, double, int, K3::base_string, int, int, double, K3::base_string,
          K3::base_string, K3::base_string, K3::base_string, K3::base_string, int, double> record;
          std::getline(in, tmp_buffer, '|');
          record.l_orderkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_partkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_suppkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_linenumber = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_quantity = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_extendedprice = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_discount = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_tax = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.l_returnflag = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.l_linestatus = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.l_shipdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.l_commitdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.l_receiptdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.l_shipinstruct = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.l_shipmode = tmp_buffer;
          std::getline(in, tmp_buffer);
          record.l_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t customerLdr(string file,
      _Collection<R_c_acctbal_c_address_c_comments_c_custkey_c_mktsegment_c_name_c_nationkey_c_phone<double,
      K3::base_string, K3::base_string, int, K3::base_string, K3::base_string, int,
      K3::base_string>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_c_acctbal_c_address_c_comments_c_custkey_c_mktsegment_c_name_c_nationkey_c_phone<double,
          K3::base_string, K3::base_string, int, K3::base_string, K3::base_string, int,
          K3::base_string> record;
          std::getline(in, tmp_buffer, '|');
          record.c_custkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.c_name = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.c_address = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.c_nationkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.c_phone = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.c_acctbal = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.c_mktsegment = tmp_buffer;
          std::getline(in, tmp_buffer);
          record.c_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t ordersLdr(string file,
      _Collection<R_o_clerk_o_comments_o_custkey_o_orderdate_o_orderkey_o_orderpriority_o_orderstatus_o_shippriority_o_totalprice<K3::base_string,
      K3::base_string, int, K3::base_string, int, K3::base_string, K3::base_string, int,
      double>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_o_clerk_o_comments_o_custkey_o_orderdate_o_orderkey_o_orderpriority_o_orderstatus_o_shippriority_o_totalprice<K3::base_string,
          K3::base_string, int, K3::base_string, int, K3::base_string, K3::base_string, int,
          double> record;
          std::getline(in, tmp_buffer, '|');
          record.o_orderkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.o_custkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.o_orderstatus = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.o_totalprice = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.o_orderdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.o_orderpriority = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.o_clerk = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.o_shippriority = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer);
          record.o_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t supplierLdr(string file,
      _Collection<R_s_acctbal_s_address_s_comments_s_name_s_nationkey_s_phone_s_suppkey<double,
      K3::base_string, K3::base_string, K3::base_string, int, K3::base_string, int>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_s_acctbal_s_address_s_comments_s_name_s_nationkey_s_phone_s_suppkey<double,
          K3::base_string, K3::base_string, K3::base_string, int, K3::base_string, int> record;
          std::getline(in, tmp_buffer, '|');
          record.s_suppkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.s_name = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.s_address = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.s_nationkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.s_phone = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.s_acctbal = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer);
          record.s_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t partsuppLdr(string file,
      _Collection<R_ps_availqty_ps_comments_ps_partkey_ps_suppkey_ps_supplycost<int,
      K3::base_string, int, int, double>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_ps_availqty_ps_comments_ps_partkey_ps_suppkey_ps_supplycost<int, K3::base_string, int,
          int, double> record;
          std::getline(in, tmp_buffer, '|');
          record.ps_partkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.ps_suppkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.ps_availqty = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.ps_supplycost = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer);
          record.ps_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t partLdr(string file,
      _Collection<R_p_brand_p_comments_p_container_p_mfgr_p_name_p_p_size_p_partkey_p_retailprice_p_type<K3::base_string,
      K3::base_string, K3::base_string, K3::base_string, K3::base_string, int, int, double,
      K3::base_string>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_p_brand_p_comments_p_container_p_mfgr_p_name_p_p_size_p_partkey_p_retailprice_p_type<K3::base_string,
          K3::base_string, K3::base_string, K3::base_string, K3::base_string, int, int, double,
          K3::base_string> record;
          std::getline(in, tmp_buffer, '|');
          record.p_partkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.p_name = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.p_mfgr = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.p_brand = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.p_type = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.p_p_size = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.p_container = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.p_retailprice = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer);
          record.p_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t nationLdr(string file,
      _Collection<R_n_comments_n_name_n_nationkey_n_regionkey<K3::base_string, K3::base_string, int,
      int>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_n_comments_n_name_n_nationkey_n_regionkey<K3::base_string, K3::base_string, int,
          int> record;
          std::getline(in, tmp_buffer, '|');
          record.n_nationkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.n_name = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.n_regionkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer);
          record.n_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t regionLdr(string file, _Collection<R_r_comments_r_name_r_regionkey<K3::base_string,
      K3::base_string, int>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_r_comments_r_name_r_regionkey<K3::base_string, K3::base_string, int> record;
          std::getline(in, tmp_buffer, '|');
          record.r_regionkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.r_name = tmp_buffer;
          std::getline(in, tmp_buffer);
          record.r_comments = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
      unit_t tpchAgendaLdr(string file,
      _Collection<R_acctbal_address_availqty_brand_clerk_comments_commitdate_container_custkey_d_size_discount_event_extendedprice_linenumber_linestatus_mfgr_mktsegment_name_nationkey_oid_orderdate_orderkey_orderpriority_orderstatus_partkey_phone_quantity_receiptdate_regionkey_retailprice_returnflag_schema_shipdate_shipinstruct_shipmode_shippriority_suppkey_supplycost_tax_totalprice_type2<double,
      K3::base_string, int, K3::base_string, K3::base_string, K3::base_string, K3::base_string,
      K3::base_string, int, int, double, int, double, int, K3::base_string, K3::base_string,
      K3::base_string, K3::base_string, int, int, K3::base_string, int, K3::base_string,
      K3::base_string, int, K3::base_string, double, K3::base_string, int, double, K3::base_string,
      K3::base_string, K3::base_string, K3::base_string, K3::base_string, int, int, double, double,
      double, K3::base_string>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_acctbal_address_availqty_brand_clerk_comments_commitdate_container_custkey_d_size_discount_event_extendedprice_linenumber_linestatus_mfgr_mktsegment_name_nationkey_oid_orderdate_orderkey_orderpriority_orderstatus_partkey_phone_quantity_receiptdate_regionkey_retailprice_returnflag_schema_shipdate_shipinstruct_shipmode_shippriority_suppkey_supplycost_tax_totalprice_type2<double,
          K3::base_string, int, K3::base_string, K3::base_string, K3::base_string, K3::base_string,
          K3::base_string, int, int, double, int, double, int, K3::base_string, K3::base_string,
          K3::base_string, K3::base_string, int, int, K3::base_string, int, K3::base_string,
          K3::base_string, int, K3::base_string, double, K3::base_string, int, double,
          K3::base_string, K3::base_string, K3::base_string, K3::base_string, K3::base_string, int,
          int, double, double, double, K3::base_string> record;
          std::getline(in, tmp_buffer, '|');
          record.oid = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.schema = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.event = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.acctbal = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.address = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.availqty = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.brand = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.clerk = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.comments = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.commitdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.container = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.custkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.discount = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.extendedprice = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.linenumber = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.linestatus = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.mfgr = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.mktsegment = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.name = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.nationkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.orderdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.orderkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.orderpriority = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.orderstatus = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.partkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.phone = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.quantity = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.receiptdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.regionkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.retailprice = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.returnflag = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.shipdate = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.shipinstruct = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.shipmode = tmp_buffer;
          std::getline(in, tmp_buffer, '|');
          record.shippriority = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.d_size = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.suppkey = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.supplycost = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.tax = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, '|');
          record.totalprice = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer);
          record.type2 = tmp_buffer;
          return record;
        });
        return unit_t {};
      }
    // TODO move to seperate context
    unit_t loadRKQ3(string_impl file, K3::Map<R_key_value<string_impl,int>>& c)  {
        // Buffers
        std::string tmp_buffer;
        R_key_value<string_impl, int> rec;
        // Infile
        std::ifstream in;
        in.open(file);

        // Parse by line
        while(!in.eof()) {
          std::getline(in, tmp_buffer, ',');
          rec.key = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          rec.value = std::atoi(tmp_buffer.c_str());
          // ignore last value
          std::getline(in, tmp_buffer);
          c.insert(rec);
        }

        return unit_t {};
    }

   template <template <class> class C, template <typename ...> class R>
   unit_t loadQ1(string_impl filepath, C<R<int, string_impl>>& c) {
        std::ifstream _in;
        _in.open(filepath);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R<int, string_impl> record;
          // Get pageURL
          std::getline(in, tmp_buffer, ',');
          record.pageURL = tmp_buffer;
          // Get pageRank
          std::getline(in, tmp_buffer, ',');
          record.pageRank = std::atoi(tmp_buffer.c_str());
          // Ignore avgDuration
          std::getline(in, tmp_buffer);
          //record.avgDuration = std::atoi(tmp_buffer.c_str());
          return record;
        });
        return unit_t {};
   }


   template <template<typename S> class C, template <typename ...> class R>
   unit_t loadQ2(string_impl filepath, C<R<double, string_impl>>& c) {
        std::ifstream _in;
        _in.open(filepath);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R<double, string_impl> record;
          // Get sourceIP
          std::getline(in, tmp_buffer, ',');
          record.sourceIP = tmp_buffer;

          // Ignore until adRevenue
          std::getline(in, tmp_buffer, ',');
          //record.destURL = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.visitDate = tmp_buffer;

          // Get adRevenue
          std::getline(in, tmp_buffer, ',');
          record.adRevenue = std::atof(tmp_buffer.c_str());

          // Ignore the rest
          std::getline(in, tmp_buffer, ',');
          //record.userAgent = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.countryCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.languageCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.searchWord = tmp_buffer;
          std::getline(in, tmp_buffer);
          //record.duration = std::atoi(tmp_buffer.c_str());
          return record;
        });
        return unit_t {};
   }

   template <template<typename S> class C, template <typename ...> class R>
   unit_t loadUVQ3(string_impl filepath, C<R<double, string_impl, string_impl, string_impl>>& c) {
        std::ifstream _in;
        _in.open(filepath);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R<double, string_impl, string_impl, string_impl> record;
          std::getline(in, tmp_buffer, ',');
          record.sourceIP = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          record.destURL = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          record.visitDate = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          record.adRevenue = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, ',');
          //record.userAgent = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.countryCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.languageCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.searchWord = tmp_buffer;
          std::getline(in, tmp_buffer);
          //record.duration = std::atoi(tmp_buffer.c_str());
          return record;
        });
        return unit_t {};
   }
    Vector<R_elem<double>> zeroVector(int i);
    Vector<R_elem<double>> randomVector(int i);

    template <template<typename S> class C, class V>
    unit_t loadVector(string_impl filepath, C<R_elem<V>>& c) {
      std::string line;
      std::ifstream infile(filepath);
      while (std::getline(infile, line)){
        char * pch;
        pch = strtok(&line[0],",");
        V v;
        while (pch) {
          R_elem<double> rec;
          rec.elem = std::atof(pch);
          v.insert(rec);
          pch = strtok(NULL,",");
        }
        R_elem<V> rec2 {v};
        c.insert(rec2);
      }
      return unit_t();

    }

    template <template<typename S> class C, template <typename ...> class R, class V>
    unit_t loadVectorLabel(int dims, string_impl filepath, C<R<double,V>>& c) {

        // Buffers
        std::string tmp_buffer;
        R<double,V>  rec;
        // Infile
        std::ifstream in;
        in.open(filepath);

        // Parse by line
        while(!in.eof()) {
          V v;
          R_elem<double> r;
          for (int j = 0; j < dims; j++) {
            std::getline(in, tmp_buffer, ',');
            r.elem = std::atof(tmp_buffer.c_str());
            v.insert(r);
          }
          std::getline(in, tmp_buffer, ',');
          rec.class_label = std::atof(tmp_buffer.c_str());
          rec.elem = v;
          c.insert(rec);

          in >> std::ws;
        }

        return unit_t {};


    }
  };

  // Utilities:


  // Time:
  class __time_context {
    public:
    __time_context();
    int now_int(unit_t);
  };

  // String operations:

  class __string_context {
    public:
    shared_ptr<RE2> pattern;
    __string_context();

    string_impl concat(string_impl s1, string_impl s2);
    string_impl itos(int i);

    string_impl rtos(double d);

    F<Collection<R_elem<string_impl>>(const string_impl &)> regex_matcher(const string_impl&);
    Collection<R_elem<string_impl>> regex_matcher_q4(const string_impl&);

    template <class S> S slice_string(const S& s, int x, int y) {
      return s.substr(x, y);
    }


    // Split a string by substrings
    Seq<R_elem<string_impl>> splitString(string_impl, const string_impl&);
    string_impl takeUntil(const string_impl& s, const string_impl& splitter);
    int countChar(const string_impl& s, const string_impl& splitter);
  };



} // namespace K3

#endif /* K3_RUNTIME_BUILTINS_H */
