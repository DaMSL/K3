#include <algorithm>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>

#include <Common.hpp>
#include <Engine.hpp>
#include <Serialization.hpp>

namespace K3
{
  using namespace std;

  string generateCollectionFilename(Engine * engine);
  string openCollectionFile(Engine * engine, const Identifier& name, IOMode mode);
  Identifier openCollectionFile(Engine * engine, const Identifier& name, IOMode mode);
  Identifier emptyFile(Engine * engine);

  template<typename T>
  shared_ptr<T> readExternal(Engine * engine, const Identifier& file_id)
  {
      shared_ptr<Value> str = engine->doReadExternal(file_id);
      if (str)
          return BoostSerializer::unpack<T>(*str);
      else
          return shared_ptr<T>(nullptr);
  }
  template<typename T>
  void writeExternal(Engine * engine, const Identifier& file_id, const T& val)
  {
      engine->doWriteExternal(file_id, BoostSerializer::pack(val));
  }
  template<typename Elem, typename AccumT>
  AccumT foldOpenFile(Engine * engine, std::function<AccumT(AccumT, Elem)> accumulation, AccumT initial_accumulator, const Identifier& file_id)
  {
    while (engine->hasRead(file_id))
    {
      shared_ptr<Elem> cur_val = readExternal<Elem>(engine, file_id);
      if (cur_val)
        initial_accumulator = accumulation(initial_accumulator, *cur_val);
      else
        return initial_accumulator;
    }
    return initial_accumulator;
  }

  template<typename Elem, typename AccumT>
  AccumT foldFile(Engine * engine, std::function<AccumT(AccumT, Elem)> accumulation, AccumT initial_accumulator, const Identifier& file_id)
  {
    openCollectionFile(engine, file_id, IOMode::Read);
    AccumT result = foldOpenFile<Elem, AccumT>(engine, accumulation, initial_accumulator, file_id);
    engine->close(file_id);
    return result;
  }

  string copyFile(Engine * engine, const string& old_id);

  template<typename Iterator>
  Identifier initialFile(Engine* engine, Iterator start, Iterator finish)
  {
    Identifier new_id = generateCollectionFilename(engine);
    openCollectionFile(engine, new_id, IOMode::Write);
    for (Iterator iter = start; iter != finish; ++iter )
    {
        engine->doWriteExternal(new_id, BoostSerializer::pack(*iter));
    }
    engine->close(new_id);
    return new_id;
  }


  template<typename Elem>
  shared_ptr<Elem> peekFile(Engine * engine, const Identifier& file_id)
  {
      openCollectionFile(engine, file_id, IOMode::Read);
      if (!engine->hasRead(file_id)) {
          engine->close(file_id);
          return shared_ptr<Elem>(nullptr);
      }
      shared_ptr<Elem> result = readExternal<Elem>(engine, file_id);
      engine->close(file_id);
      return result;
  }

  template<typename Elem>
  static void iterateOpenFile(Engine * engine, std::function<void(Elem)> f, const Identifier& file_id)
  {
      while (engine->hasRead(file_id))
      {
          shared_ptr<Elem> cur_val = readExternal<Elem>(engine, file_id);
          if (cur_val)
            f(*cur_val);
      }
  }
  template<typename Elem>
  void iterateFile_noCopy(Engine * engine, std::function<void(Elem)> function, const Identifier& file_id)
  {
      openCollectionFile(engine, file_id, IOMode::Read);
      iterateOpenFile(engine, function, file_id);
      engine->close(file_id);
  }

  template<typename Elem, typename Output>
  Identifier mapFile(Engine * engine, std::function<Output(Elem)> function, const Identifier& file_ds)
  {
      Identifier tmp_ds = copyFile(engine, file_ds);
      Identifier new_id = generateCollectionFilename(engine);
      openCollectionFile(engine, new_id, IOMode::Write);
      iterateFile_noCopy<Elem>(engine,
              [engine, &new_id, &function](Elem val) {
                  writeExternal(engine, new_id, function(val));
              }, tmp_ds);
      engine->close(new_id);
      return new_id;
  }
  template<typename Elem>
  void mapFile_(Engine * engine, std::function<void(Elem)> function, const Identifier& file_ds)
  {
      Identifier tmp_ds = copyFile(engine, file_ds);
      iterateFile_noCopy<Elem>(engine, [engine, &function](Elem val) {
                  function(val);
              }, tmp_ds);
  }

  template<typename Elem>
  Identifier filterFile(Engine * engine, std::function<bool(Elem)> predicate, const Identifier& old_ds)
  {
      Identifier tmp_ds = copyFile(engine, old_ds);
      Identifier new_id = generateCollectionFilename(engine);
      openCollectionFile(engine, new_id, IOMode::Write);
      iterateFile_noCopy<Elem>(engine,
              [engine, &new_id, &predicate](Elem val) {
                  if (predicate(val))
                      writeExternal(engine, new_id, val);
              }, tmp_ds);
      engine->close(new_id);
      return new_id;
  }

  template<typename Elem>
  Identifier insertFile(Engine * engine, const Identifier& old_id, const Elem& v)
  {
      openCollectionFile(engine, old_id, IOMode::Append);
      writeExternal(engine, old_id, v);
      engine->close(old_id);
      return old_id;
  }

  template<typename Elem>
  Identifier eraseFile(Engine * engine, const Identifier& old_id, const Elem& v)
  {
      Identifier deleted_id = generateCollectionFilename(engine);
      openCollectionFile(engine, deleted_id, IOMode::Write);
      foldFile<Elem, bool>(engine,
              [engine, &deleted_id, v](bool found, Elem val) {
                  if (!found && val == v)
                      return true;
                  else {
                      writeExternal(engine, deleted_id, val);
                      return found;
                  }
              }, false, old_id);
      engine->close(deleted_id);
      boost::filesystem::rename(deleted_id, old_id);
      return old_id;
  }

  template<typename Elem>
  Identifier updateFile(Engine * engine, const Elem& old_val, const Elem& new_val, const Identifier& file_id)
  {
      Identifier new_id = generateCollectionFilename(engine);
      openCollectionFile(engine, new_id, IOMode::Write);
      bool did_update = foldFile<Elem, bool>(engine,
              [engine, &new_id, old_val, new_val](bool found, Elem val) {
                  if (!found && val == old_val) {
                      writeExternal(engine, new_id, new_val);
                      return true;
                  }
                  else {
                      writeExternal(engine, new_id, val);
                      return found;
                  }
              }, false, file_id);
      if (!did_update)
          writeExternal(engine, new_id, new_val);
      engine->close(new_id);
      boost::filesystem::rename(new_id, file_id);
      return file_id;
  }

  template<typename Elem>
  Identifier combineFile(Engine * engine, const Identifier& self, const Identifier& values)
  {
      // TODO Can I get away without unpacking and packing the values again here?
      Identifier new_id = copyFile(engine, self);
      openCollectionFile(engine, new_id, IOMode::Append);
      iterateFile_noCopy<Elem>(engine,
              [engine, &new_id](Elem v) {
                  writeExternal(engine, new_id, v);
              }, values);
      engine->close(new_id);
      return new_id;
  }

  template<typename Elem>
  tuple<Identifier, Identifier> splitFile(Engine * engine, const Identifier& file_id)
  {
      Identifier left = openCollectionFile(engine, generateCollectionFilename(engine), IOMode::Write);
      Identifier right = openCollectionFile(engine, generateCollectionFilename(engine), IOMode::Write);
      foldFile<Elem, bool>(engine,
              [engine, &left, &right](bool use_left_file, Elem cur_val) {
                  if (use_left_file) {
                      writeExternal(engine, left, cur_val);
                      return false;
                  }
                  else {
                      writeExternal(engine, right, cur_val);
                      return true;
                  }
              }, true, file_id);
      engine->close(left);
      engine->close(right);
      return make_tuple(left, right);
  }

  template<typename T>
  class FileDS
  {
      protected:
      Engine * engine;
      Identifier file_id;

      public:
      typedef T Elem;
      FileDS(Engine * eng)
          : engine(eng), file_id(emptyFile(eng))
      { }

      template<typename Iterator>
      FileDS(Engine * eng, Iterator start, Iterator finish)
          : engine(eng), file_id(initialFile(eng, start, finish))
      { }

      FileDS(const FileDS& other)
          : engine(other.engine), file_id(copyFile(other.engine, other.file_id))
      { }

    private:
      FileDS(Engine * eng, Identifier f)
          : engine(eng), file_id(f)
      { }

    public:
      shared_ptr<Elem> peek() const
      {
        return peekFile<Elem>(engine, file_id);
      }

      void insert(const Elem& v)
      {
        file_id = insertFile(engine, file_id, v);
      }
      void erase(const Elem& v)
      {
        file_id = eraseFile(engine, file_id, v);
      }
      void update(const Elem& v, const Elem& v2)
      {
        file_id = updateFile(engine, v, v2, file_id);
      }

      template<typename Accum>
      Accum fold(std::function<Accum(Accum, Elem)> accum, Accum initial_accumulator)
      {
        return foldFile<Elem, Accum>(engine, accum, initial_accumulator, file_id);
      }

      template<typename Result>
      FileDS map(std::function<Result(Elem)> func)
      {
        return FileDS(engine,
                mapFile<Elem, Result>(engine, func, file_id)
                );
      }

      void iterate(std::function<void(Elem)> func)
      {
        mapFile_(engine, func, file_id);
      }

      FileDS filter(std::function<bool(Elem)> predicate)
      {
        return FileDS(engine,
                filterFile(engine, predicate, file_id)
                );
      }

      tuple< FileDS, FileDS > split()
      {
        tuple<Identifier, Identifier> halves = splitFile<Elem>(engine, file_id);
        return make_tuple(
                FileDS(engine, get<0>(halves)),
                FileDS(engine, get<1>(halves))
                );
      }
      FileDS combine(FileDS other)
      {
        return FileDS(engine,
                combineFile<Elem>(engine, file_id, other.file_id)
                );
      }
  };
};
