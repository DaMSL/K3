#include <string>
#include <boost/filesystem.hpp>
#include <dataspace/FileDataspace.hpp>

namespace K3
{
  using namespace std;
  static const boost::filesystem::path rootDataPath = "_DATA";

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

  Identifier copyFile(Engine * engine, const Identifier& old_id)
  {
      Identifier new_id = generateCollectionFilename(engine);
      openCollectionFile(engine, new_id, IOMode::Write);
      mapFile_(engine,
              [engine, &new_id](Value v) {
                  engine->doWriteExternal(new_id, v);
              }, old_id);
      engine->close(new_id);
      return new_id;
  }

  shared_ptr<Value> peekFile(Engine * engine, const Identifier& file_id)
  {
      openCollectionFile(engine, file_id, IOMode::Read);
      if (!engine->hasRead(file_id))
          return shared_ptr<Value>(nullptr);
      shared_ptr<Value> result = engine->doReadExternal(file_id);
      engine->close(file_id);
      return result;
  }

  Identifier mapFile(Engine * engine, std::function<Value(Value)> function, const Identifier& file_ds)
  {
      Identifier tmp_ds = copyFile(engine, file_ds);
      Identifier new_id = generateCollectionFilename(engine);
      openCollectionFile(engine, new_id, IOMode::Write);
      mapFile_(engine,
              [engine, &new_id, &function](Value val) {
                  engine->doWriteExternal(new_id, function(val));
              }, tmp_ds);
      engine->close(new_id);
      return new_id;
  }
  void mapFile_(Engine * engine, std::function<void(Value)> function, const Identifier& file_ds)
  {
      Identifier tmp_ds = copyFile(engine, file_ds);
      foldFile<tuple<>>(engine,
              [engine, &function](tuple<>, Value val) {
                  function(val);
                  return tuple<>();
              }, tuple<>(), tmp_ds);
  }

  Identifier filterFile(Engine * engine, std::function<bool(Value)> predicate, const Identifier& old_ds)
  {
      Identifier tmp_ds = copyFile(engine, old_ds);
      Identifier new_id = generateCollectionFilename(engine);
      openCollectionFile(engine, new_id, IOMode::Write);
      mapFile_(engine,
              [engine, &new_id, &predicate](Value val) {
                  if (predicate(val))
                      engine->doWriteExternal(new_id, val);
              }, tmp_ds);
      engine->close(new_id);
      return new_id;
  }

  Identifier insertFile(Engine * engine, const Identifier& old_id, const Value& v)
  {
      openCollectionFile(engine, old_id, IOMode::Append);
      engine->doWriteExternal(old_id, v);
      engine->close(old_id);
      return old_id;
  }

  Identifier deleteFile(Engine * engine, const Identifier& old_id, const Value& v)
  {
      Identifier deleted_id = generateCollectionFilename(engine);
      openCollectionFile(engine, deleted_id, IOMode::Write);
      foldFile<bool>(engine,
              [engine, &deleted_id, v](bool found, Value val) {
                  if (!found && val == v)
                      return true;
                  else {
                      engine->doWriteExternal(deleted_id, val);
                      return found;
                  }
              }, false, old_id);
      engine->close(deleted_id);
      boost::filesystem::rename(deleted_id, old_id);
      return old_id;
  }

  Identifier updateFile(Engine * engine, const Value& old_val, const Value& new_val, const Identifier& file_id)
  {
      Identifier new_id = generateCollectionFilename(engine);
      openCollectionFile(engine, new_id, IOMode::Write);
      bool did_update = foldFile<bool>(engine,
              [engine, &new_id, old_val, new_val](bool found, Value val) {
                  if (!found && val == old_val) {
                      engine->doWriteExternal(new_id, new_val);
                      return true;
                  }
                  else {
                      engine->doWriteExternal(new_id, val);
                      return found;
                  }
              }, false, file_id);
      if (did_update)
          engine->doWriteExternal(new_id, new_val);
      engine->close(new_id);
      boost::filesystem::rename(new_id, file_id);
      return file_id;
  }

  // TODO map
  Identifier combineFile(Engine * engine, const Identifier& self, const Identifier& values)
  {
      Identifier new_id = copyFile(engine, self);
      openCollectionFile(engine, new_id, IOMode::Append);
      mapFile_(engine,
              [engine, &new_id](Value v) {
                  engine->doWriteExternal(new_id, v);
              }, values);
      engine->close(new_id);
      return new_id;
  }

  tuple<Identifier, Identifier> splitFile(Engine * engine, const Identifier& file_id)
  {
      Identifier left = openCollectionFile(engine, generateCollectionFilename(engine), IOMode::Write);
      Identifier right = openCollectionFile(engine, generateCollectionFilename(engine), IOMode::Write);
      foldFile<bool>(engine,
              [engine, &left, &right](bool use_left_file, Value cur_val) {
                  if (use_left_file) {
                      engine->doWriteExternal(left, cur_val);
                      return false;
                  }
                  else {
                      engine->doWriteExternal(right, cur_val);
                      return true;
                  }
              }, true, file_id);
      engine->close(left);
      engine->close(right);
      return make_tuple(left, right);
  }
};