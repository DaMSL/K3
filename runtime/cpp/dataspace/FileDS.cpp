#include <dataspace/FileDS.hpp>

namespace K3
{
  using namespace std;
  static const boost::filesystem::path rootDataPath = "_DATA";

  template<typename T>
  string toString(const T& not_string)
  {
    ostringstream strm;
    strm << not_string;
    return strm.str();
  }

  boost::filesystem::path engineDataPath(Engine * engine)
  {
    string addrString = addressAsString(engine->getAddress());
    replace(std::begin(addrString), std::end(addrString), ':', '_');
    return (rootDataPath / addrString);
  }

  void createDataDir(const boost::filesystem::path& path)
  {
    bool dirExists = boost::filesystem::is_directory(path);
    if (!dirExists) {
      if (boost::filesystem::exists(path)) {
          throw runtime_error(path.string() + " exists but is not a directory, so it cannot be used to store external collections.");
      }
      else {
          boost::filesystem::create_directory(path);
      }
    }
  }

  static void initDataDir(Engine * engine)
  {
    createDataDir(rootDataPath);
    createDataDir(engineDataPath(engine));
  }

  // TODO use boost::filesystem::path?
  Identifier generateCollectionFilename(Engine * engine)
  {
    initDataDir(engine);
    unsigned counter = engine->getCollectionCount();
    Identifier filename = "collection_" + toString(counter);
    engine->incrementCollectionCount();
    return (engineDataPath(engine) / filename).string();
  }

  Identifier openCollectionFile(Engine * engine, const Identifier& name, IOMode mode)
  {
    engine->openFile(name, name, mode);
    return name;
  }

  Identifier emptyFile(Engine * engine)
  {
      Identifier file_id = generateCollectionFilename(engine);
      openCollectionFile(engine, file_id, IOMode::Write);
      engine->close(file_id);
      return file_id;
  }

}
