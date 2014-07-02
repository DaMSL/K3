#include "Common.hpp"

using namespace std;

namespace K3
{
      Value DelimiterCodec::encode(const Value& v) {
        string res = string(v);
        res.push_back(delimiter_);
        return res;
      }

      shared_ptr<Value> DelimiterCodec::decode(const Value& v) {

        // Append to buffer
        buf_->append(v);
        // Determine if there is a complete value in the buffer
        shared_ptr<Value> result = shared_ptr<Value>();
        size_t pos = find_delimiter();
        if (pos != std::string::npos) {
          // There is a complete value
          // Grab it from the buffer
          result = shared_ptr<string>(new string());
          *result = buf_->substr(0, pos); // ignore the delimiter at pos
          // Delete from the buffer
          *buf_ = buf_->substr(pos+1);
        }
        return result;
      }

      Value LengthHeaderCodec::encode(const Value& s) {
        // calculate size of encoded value
        fixed_int value_size = fixed_int(s.length());
        size_t header_size = sizeof(value_size);
        size_t enc_size = header_size + value_size;
        // pack data into a buffer
        char * buffer = new char[enc_size]();
        memcpy(buffer, &value_size, header_size);
        memcpy(buffer + header_size, s.c_str(), value_size);
        // copy into string and free buffer
        Value enc_v = string(buffer, enc_size);
        delete[] buffer;

        return enc_v;
      }

      shared_ptr<Value> LengthHeaderCodec::decode(const Value& v) {

        if (v != "") {
        *buf_ = *buf_ + v;
        }

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
          const char * bytes = buf_->c_str();
          fixed_int i = *next_size_;
          shared_ptr<Value> result = shared_ptr<Value>(new string(bytes, i));

          // Setup for next round
          *buf_ = buf_->substr(i);
          next_size_.reset();
          return result;
        }
        else {
          // failure: not enough data in buffer
          return nullptr;
        }
      }

      void LengthHeaderCodec::strip_header() {
        Value s = *buf_;
        size_t header_size = sizeof(fixed_int);
        if (s.length() < header_size) {
          // failure: input does not contain a full header
          return;
        }
        const char * bytes = s.c_str();
        // copy the fixed_int into next_size_
        fixed_int * n = new fixed_int();
        memcpy(n, bytes, header_size);
        next_size_ = shared_ptr<fixed_int>(new fixed_int(*n));
        delete n;

        // remove the header bytes from the buffer
        *buf_ = buf_->substr(header_size);
      }

      Message AbstractDefaultInternalCodec::read_message(const Value& v) {
        // Values are of the form: "(Address, Identifier, Payload)"
        // Split value into components:
        static const boost::regex value_regex("\\( *(.+) *, *(.+) *, *(.+) *\\)");
        //logAt(trivial::trace, v);
        boost::cmatch value_match;
        if(boost::regex_match(v.c_str(), value_match, value_regex)){
          // Parse Address
          static const boost::regex address_regex("(.+):(.+)");
          boost::cmatch address_match;
          Address a;
          string temp = value_match[1];
          if(boost::regex_match(temp.c_str(), address_match, address_regex)) {
            string ip = address_match[1];
            temp = address_match[2];
            unsigned short port = (unsigned short) std::strtoul(temp.c_str(), NULL, 0);
            a = make_address(ip, port);
          }
          else {
            throw CodecException("Invalid Format for Value's Address: " + value_match[1]);
          }

          // Parse Identifier
          Identifier m = value_match[2];
          // Parse Payload
          Value payload = value_match[3];
          return Message(a,m,payload);
         }
        else {
          throw CodecException("Invalid Format for Value:" + v);
        }
      }

      Value AbstractDefaultInternalCodec::show_message(const Message& m) {
        ostringstream os;
        os << "(" << addressAsString(m.address()) << "," << m.id() << "," << m.contents() << ")";
        string s = os.str();
        return s;
      }


}
