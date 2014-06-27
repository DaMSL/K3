#ifndef K3_RUNTIME_DATASPACE_FILEDS_H
#define K3_RUNTIME_DATASPACE_FILEDS_H

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
  class FileDS
  {
      public:
      typedef T Elem;

      protected:
      Engine * engine;
      Identifier file_id;

      static shared_ptr<Elem> readExternal(Engine * engine, const Identifier& file_id)
      {
          shared_ptr<Value> str = engine->doReadExternal(file_id);
          if (str)
              return BoostSerializer::unpack<Elem>(*str);
          else
              return shared_ptr<Elem>(nullptr);
      }

      static void writeExternal(Engine * engine, const Identifier& file_id, const Elem& val)
      {
          engine->doWriteExternal(file_id, BoostSerializer::pack(val));
      }

      template<typename AccumT>
      static AccumT foldOpenFile(Engine * engine, F<F<AccumT(Elem)>(AccumT)> accumulation, AccumT initial_accumulator, const Identifier& file_id)
      {
        while (engine->hasRead(file_id))
        {
          shared_ptr<Elem> cur_val = readExternal(engine, file_id);
          if (cur_val)
            initial_accumulator = accumulation(initial_accumulator)(*cur_val);
          else
            return initial_accumulator;
        }
        return initial_accumulator;
      }

      template<typename AccumT>
      static AccumT foldFile(Engine * engine, F<F<AccumT(Elem)>(AccumT)> accumulation, AccumT initial_accumulator, const Identifier& file_id)
      {
        openCollectionFile(engine, file_id, IOMode::Read);
        AccumT result = foldOpenFile<AccumT>(engine, accumulation, initial_accumulator, file_id);
        engine->close(file_id);
        return result;
      }

      template<typename Iterator>
      static Identifier initialFile(Engine* engine, Iterator start, Iterator finish)
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

      static shared_ptr<Elem> peekFile(Engine * engine, const Identifier& file_id)
      {
          openCollectionFile(engine, file_id, IOMode::Read);
          if (!engine->hasRead(file_id)) {
              engine->close(file_id);
              return shared_ptr<Elem>(nullptr);
          }
          shared_ptr<Elem> result = readExternal(engine, file_id);
          engine->close(file_id);
          return result;
      }

      static void iterateOpenFile(Engine * engine, F<unit_t(Elem)> f, const Identifier& file_id)
      {
          while (engine->hasRead(file_id))
          {
              shared_ptr<Elem> cur_val = readExternal(engine, file_id);
              if (cur_val)
                f(*cur_val);
          }
      }

      static void iterateFile_noCopy(Engine * engine, F<unit_t(Elem)> function, const Identifier& file_id)
      {
          openCollectionFile(engine, file_id, IOMode::Read);
          iterateOpenFile(engine, function, file_id);
          engine->close(file_id);
      }

      static Identifier copyFile(Engine * engine, const Identifier& old_id)
      {
          Identifier new_id = generateCollectionFilename(engine);
          openCollectionFile(engine, new_id, IOMode::Write);
          iterateFile_noCopy(engine,
                  [engine, &new_id](Elem v) {
                      writeExternal(engine, new_id, v);
                  }, old_id);
          engine->close(new_id);
          return new_id;
      }

      template<typename Output>
      static Identifier mapFile(Engine * engine, F<Output(Elem)> function, const Identifier& file_ds)
      {
          Identifier tmp_ds = copyFile(engine, file_ds);
          Identifier new_id = generateCollectionFilename(engine);
          openCollectionFile(engine, new_id, IOMode::Write);
          iterateFile_noCopy(engine,
                  [engine, &new_id, &function](Elem val) {
                      writeExternal(engine, new_id, function(val));
                  }, tmp_ds);
          engine->close(new_id);
          return new_id;
      }
      static void mapFile_(Engine * engine, F<unit_t(Elem)> function, const Identifier& file_ds)
      {
          Identifier tmp_ds = copyFile(engine, file_ds);
          iterateFile_noCopy(engine, [engine, &function](Elem val) {
                      function(val);
                  }, tmp_ds);
      }

      static Identifier filterFile(Engine * engine, F<bool(Elem)> predicate, const Identifier& old_ds)
      {
          Identifier tmp_ds = copyFile(engine, old_ds);
          Identifier new_id = generateCollectionFilename(engine);
          openCollectionFile(engine, new_id, IOMode::Write);
          iterateFile_noCopy(engine,
                  [engine, &new_id, &predicate](Elem val) {
                      if (predicate(val))
                          writeExternal(engine, new_id, val);
                  }, tmp_ds);
          engine->close(new_id);
          return new_id;
      }

      static Identifier insertFile(Engine * engine, const Identifier& old_id, const Elem& v)
      {
          openCollectionFile(engine, old_id, IOMode::Append);
          writeExternal(engine, old_id, v);
          engine->close(old_id);
          return old_id;
      }

      static Identifier eraseFile(Engine * engine, const Identifier& old_id, const Elem& v)
      {
          Identifier deleted_id = generateCollectionFilename(engine);
          openCollectionFile(engine, deleted_id, IOMode::Write);
          foldFile<bool>(engine,
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

      static Identifier updateFile(Engine * engine, const Elem& old_val, const Elem& new_val, const Identifier& file_id)
      {
          Identifier new_id = generateCollectionFilename(engine);
          openCollectionFile(engine, new_id, IOMode::Write);
          bool did_update = foldFile<bool>(engine,
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

      static Identifier combineFile(Engine * engine, const Identifier& self, const Identifier& values)
      {
          // TODO Can I get away without unpacking and packing the values again here?
          Identifier new_id = copyFile(engine, self);
          openCollectionFile(engine, new_id, IOMode::Append);
          iterateFile_noCopy(engine,
                  [engine, &new_id](Elem v) {
                      writeExternal(engine, new_id, v);
                  }, values);
          engine->close(new_id);
          return new_id;
      }

      static tuple<Identifier, Identifier> splitFile(Engine * engine, const Identifier& file_id)
      {
          Identifier left = openCollectionFile(engine, generateCollectionFilename(engine), IOMode::Write);
          Identifier right = openCollectionFile(engine, generateCollectionFilename(engine), IOMode::Write);
          foldFile<bool>(engine,
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

      public:

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

      FileDS(FileDS && other)
          : engine(std::move(other.engine)), file_id(std::move(other.file_id))
      { }

    private:
      FileDS(Engine * eng, Identifier f)
          : engine(eng), file_id(f)
      { }

    public:
      shared_ptr<Elem> peek() const
      {
        return peekFile(engine, file_id);
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

      Accum fold(F<F<Accum(Elem)>(Accum)> accum, Accum initial_accumulator)
      {
        return foldFile<Accum>(engine, accum, initial_accumulator, file_id);
      }

      template<typename Result>
      FileDS map(F<Result(Elem)> func)
      {
        return FileDS(engine,
                mapFile<Result>(engine, func, file_id)
                );
      }

      void iterate(F<unit_t(Elem)> func)
      {
        mapFile_(engine, func, file_id);
      }

      FileDS filter(F<bool(Elem)> predicate)
      {
        return FileDS(engine,
                filterFile(engine, predicate, file_id)
                );
      }

      tuple< FileDS, FileDS > split()
      {
        tuple<Identifier, Identifier> halves = splitFile(engine, file_id);
        return make_tuple(
                FileDS(engine, get<0>(halves)),
                FileDS(engine, get<1>(halves))
                );
      }
      FileDS combine(FileDS other)
      {
        return FileDS(engine,
                combineFile(engine, file_id, other.file_id)
                );
      }

      FileDS& operator=(const FileDS& other)
      {
          file_id = copyFile(engine, other.file_id);
      }

      FileDS& operator=(FileDS && other)
      {
          file_id = std::move(other.file_id);
          return *this;
      }

    private:
      friend class boost::serialization::access;
      template<class Archive>
      void serialize(Archive& ar, unsigned int /*version*/)
      {
          ar & file_id;
      }
  };
};

#endif // K3_RUNTIME_DATASPACE_FILEDS_H
