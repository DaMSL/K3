#ifndef K3_AMPLAB
#define K3_AMPLAB

#include <string>

#include "Common.hpp"
#include "collections/Map.hpp"
#include "builtins/loaders/ReadRecords.hpp"

namespace K3 {

class AmplabLoaders {
 public:
  AmplabLoaders() {}

  template <class C1, template <class> class C, template <typename...> class R>
  unit_t loadQ1(const C1& paths, C<R<int, string_impl>>& c) {
    K3::read_records(paths, c, [](std::istream& in, std::string& tmp_buffer) {
      std::cout << "reading one!" << std::endl;
      R<int, string_impl> record;
      // Get pageURL
      std::getline(in, tmp_buffer, ',');
      record.pageURL = tmp_buffer;
      // Get pageRank
      std::getline(in, tmp_buffer, ',');
      record.pageRank = std::atoi(tmp_buffer.c_str());
      // Ignore avgDuration
      std::getline(in, tmp_buffer);
      // record.avgDuration = std::atoi(tmp_buffer.c_str());
      return record;
    });
    return unit_t{};
  }

  template <class C1, template <typename S> class C,
            template <typename...> class R>
  unit_t loadQ2(const C1& paths, C<R<double, string_impl>>& c) {
    K3::read_records(paths, c, [](std::istream& in, std::string& tmp_buffer) {
      R<double, string_impl> record;
      // Get sourceIP
      std::getline(in, tmp_buffer, ',');
      record.sourceIP = tmp_buffer;

      // Ignore until adRevenue
      std::getline(in, tmp_buffer, ',');
      // record.destURL = tmp_buffer;
      std::getline(in, tmp_buffer, ',');
      // record.visitDate = tmp_buffer;

      // Get adRevenue
      std::getline(in, tmp_buffer, ',');
      record.adRevenue = std::atof(tmp_buffer.c_str());

      // Ignore the rest
      std::getline(in, tmp_buffer);
      return record;
    });
    return unit_t{};
  }

  template <class C1, template <typename S> class C,
            template <typename...> class R>
  unit_t loadUVQ3(const C1& paths,
                  C<R<double, string_impl, string_impl, string_impl>>& c) {
    K3::read_records(paths, c, [](std::istream& in, std::string& tmp_buffer) {
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
      std::getline(in, tmp_buffer);
      return record;
    });
    return unit_t{};
  }

  template <class C1>
  unit_t loadRKQ3(const C1& paths, K3::Map<R_key_value<string_impl, int>>& c) {
    for (auto r : paths) {
      // Buffers
      std::string tmp_buffer;
      R_key_value<string_impl, int> rec;
      // Infile
      std::ifstream in;
      in.open(r.path);

      // Parse by line
      while (!in.eof()) {
        std::getline(in, tmp_buffer, ',');
        rec.key = tmp_buffer;
        std::getline(in, tmp_buffer, ',');
        rec.value = std::atoi(tmp_buffer.c_str());
        // ignore last value
        std::getline(in, tmp_buffer);
        c.insert(rec);
      }
    }

    return unit_t{};
  }
};

}  // namespace K3

#endif
