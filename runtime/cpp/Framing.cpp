#include <iostream>
#include <cstdlib>
#include "Common.hpp"
#include "Framing.hpp"

using namespace std;

namespace K3 {

  // DelimiterFraming

  Value DelimiterFraming::encode(const Value& v) {
    string res(v);
    res.push_back(delimiter_);
    return res;
  }

  // Run right after appending to buffer
  shared_ptr<Value> DelimiterFraming::completeDecode() {
    // Determine if there is a complete value in the buffer
    shared_ptr<Value> result;
    size_t pos = find_delimiter();
    if (pos != std::string::npos) {
      // There is a complete value
      // Grab it from the buffer
      result = shared_ptr<string>(new string());
      *result = buf_->substr(0, pos);  // ignore the delimiter at pos
      // Delete from the buffer
      *buf_ = buf_->substr(pos + 1);
    }
    return result;
  }

  // LengthHeaderFraming

  Value LengthHeaderFraming::encode(const Value& s) {
    // calculate size of encoded value
    fixed_int value_size(s.length());
    size_t header_size = sizeof(value_size);
    string value_size_s((char*)&value_size, header_size);
    // pack data into a buffer
    string buf;
    buf.resize(header_size + value_size);
    std::copy(value_size_s.begin(), value_size_s.end(), buf.begin());
    std::copy(s.begin(), s.end(), buf.begin() + header_size);

    return buf;
  }

  shared_ptr<Value> LengthHeaderFraming::completeDecode() {
    if (!next_size_) {
      // See if there is enough data in buffer to unpack a header
      strip_header();
      if (!next_size_) {
        // failure: not enough data in buffer
        return nullptr;
      }
    }

    // Now that we know the size of the next incoming value
    // See if the buffer contains enough data to unpack
    if (decode_ready()) {
      // Unpack next value
      fixed_int i = *next_size_;
      shared_ptr<Value> result = make_shared<Value>(buf_->c_str(), i);

      // Setup for next round
      *buf_ = buf_->substr(i);
      next_size_.reset();
      return result;
    } else {
      // failure: not enough data in buffer
      return nullptr;
    }
  }

  void LengthHeaderFraming::strip_header() {
    size_t header_size = sizeof(fixed_int);
    if (buf_->length() < header_size) {
      // failure: input does not contain a full header
      return;
    }
    // copy the fixed_int into next_size_
    fixed_int n;
    memcpy(&n, buf_->c_str(), header_size);
    next_size_ = make_shared<fixed_int>(n);

    // remove the header bytes from the buffer
    *buf_ = buf_->substr(header_size);
  }

  RemoteMessage AbstractDefaultInternalFraming::read_message(const Value& v) {
    return *BoostSerializer::unpack<RemoteMessage>(v);
  }

  Value AbstractDefaultInternalFraming::show_message(const RemoteMessage& m) {
    return BoostSerializer::pack<RemoteMessage>(m);
  }
}
