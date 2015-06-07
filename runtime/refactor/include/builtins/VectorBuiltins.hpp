#ifndef K3_VECTORBUILTINS
#define K3_VECTORBUILTINS

#include <fstream>

#include "Common.hpp"
#include "collections/Vector.hpp"
#include "types/BaseString.hpp"

namespace K3 {

class VectorBuiltins {
 public:
  VectorBuiltins();
  Vector<R_elem<double>> zeroVector(int i);
  Vector<R_elem<double>> randomVector(int i);
  template <template <typename S> class C, class V>
  unit_t loadVector(string_impl filepath, C<R_elem<V>>& c) {
    std::string line;
    std::ifstream infile(filepath);
    char* saveptr;

    while (std::getline(infile, line)) {
      char* pch;
      pch = strtok_r(&line[0], ",", &saveptr);
      V v;
      while (pch) {
        R_elem<double> rec;
        rec.elem = std::atof(pch);
        v.insert(rec);
        pch = strtok_r(NULL, ",", &saveptr);
      }
      R_elem<V> rec2{v};
      c.insert(rec2);
    }
    return unit_t();
  }

  template <template <typename S> class C, template <typename...> class R,
            class V>
  unit_t loadVectorLabel(int dims, string_impl filepath, C<R<double, V>>& c) {
    // Buffers
    std::string tmp_buffer;
    R<double, V> rec;
    // Infile
    std::ifstream in;
    in.open(filepath);
    char* saveptr;

    // Parse by line
    while (!in.eof()) {
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

    return unit_t{};
  }
  template <template <class> class M, template <class> class C,
            template <typename...> class R, class C2>
  unit_t loadGraph(const C2& filepaths, M<R<int, C<R_elem<int>>>>& c) {
    for (const auto& filepath : filepaths) {
      std::string tmp_buffer;
      std::ifstream in(filepath.path);

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
          edge_list.insert(R_elem<int>(
              std::atoi(tmp_buffer.substr(start, end - start).c_str())));
          start = end + 1;
        }

        c.insert(R<int, C<R_elem<int>>>{source, std::move(edge_list)});
        in >> std::ws;
      }
    }
    return unit_t{};
  }
};

}  // namespace K3

#endif
