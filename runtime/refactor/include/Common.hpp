#ifndef K3_COMMON
#define K3_COMMON

#include <vector>


typedef int TriggerID;
typedef std::vector<char> Buffer;

class EndOfProgramException: public std::runtime_error {
 public:
  EndOfProgramException() : runtime_error( "Peer terminated." ) { }
};

#endif
