#include "IOHandle.hpp"

namespace K3
{
    void IStreamHandle::doPrefetch() {
      while ( !pending_result && frame->good() ) {
        if (frame->decode_ready()) {
          // return a buffered value if possible
          pending_result = frame->decode("");
        }
        else if ( input_->good() ) {
            // no buffered values: grab data from stream
            char * buffer = new char[1024]();
            input_->read(buffer,sizeof(buffer));
            string s = string(buffer);
            // use the new data to attempt a decode.
            // if it fails: continue the loop
            pending_result = frame->decode(s);
            delete[] buffer;
        }
        else {
          // End of stream data
          BOOST_LOG(*this) << "doPrefetch: end of stream";
          pending_result.reset();
          return;
        }
      }
    }
}
