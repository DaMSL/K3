#ifndef K3_READRECORDS
#define K3_READRECORDS

#include <string>
#include <fstream>

namespace K3 {

template <class C1, class C, class F>
void read_records(C1& paths, C& container, F read_record) {
  for (auto rec : paths) {
    std::ifstream in;
    in.open(rec.path);
    std::string tmp_buffer;
    while (!in.eof()) {
      container.insert(read_record(in, tmp_buffer));
      in >> std::ws;
    }
  }

  return;
}

}  // namespace K3

#endif
