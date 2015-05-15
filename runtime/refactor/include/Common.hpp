#ifndef K3_COMMON
#define K3_COMMON

#include <vector>

#include <boost/asio.hpp>

typedef int TriggerID;
typedef std::vector<char> Buffer;

typedef std::tuple<boost::asio::ip::address, unsigned short> Address;

inline Address make_address(const std::string& host, unsigned short port) {
  return Address(boost::asio::ip::address::from_string(host), port);
}

inline Address make_address(unsigned long host, unsigned short port) {
  auto v4 = boost::asio::ip::address_v4(host);
  return Address(boost::asio::ip::address(v4), port);
}

class EndOfProgramException: public std::runtime_error {
 public:
  EndOfProgramException() : runtime_error( "Peer terminated." ) { }
};

#endif
