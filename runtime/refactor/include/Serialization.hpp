#ifndef K3_SERIALIZATION
#define K3_SERIALIZATION

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/iostreams/stream_buffer.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

#include <vector>
#include <iostream>

namespace Serialization {

namespace io = boost::iostreams;
typedef io::stream<io::back_insert_device<Buffer> > OByteStream;

template <class T>
void pack_into(const T& t, Buffer& buf) {
  OByteStream output_stream(buf);
  boost::archive::binary_oarchive oa(output_stream);
  oa << t;
  output_stream.flush();
  return;
}

template <class T>
Buffer pack(const T& t) {
  Buffer buf;
  pack_into(t, buf);
  return buf;
}

template <class T>
T unpack(const char* buf, size_t len) {
  io::basic_array_source<char> source(buf, len);
  io::stream<io::basic_array_source <char> > input_stream(source);
  boost::archive::binary_iarchive ia(input_stream);

  T t;
  ia >> t;
  return t;
}

template <class T>
T unpack(const Buffer& buf) {
  return unpack<T>(&buf[0], buf.size());
}

}

#endif
