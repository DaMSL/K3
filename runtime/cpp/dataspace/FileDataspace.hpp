#include <algorithm>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include <Common.hpp>
#include <Engine.hpp>

namespace K3
{
  using namespace std;

  template<typename T>
  string toString(const T& not_string)
  {
    ostringstream strm;
    strm << not_string;
    return strm.str();
  }

  template<>
  string toString(const Address& not_string);

  template<typename T>
  T fromString(string str)
  {
    istringstream strm(str);
    T not_string;
    strm >> not_string;
    return not_string;
  }

  string generateCollectionFilename(Engine * engine);
  string openCollectionFile(Engine * engine, const Identifier& name, IOMode mode);
  Identifier openCollectionFile(Engine * engine, const Identifier& name, IOMode mode);
  Identifier emptyFile(Engine * engine);

  template<typename AccumT>
  AccumT foldOpenFile(Engine * engine, std::function<AccumT(AccumT, Value)> accumulation, AccumT initial_accumulator, const Identifier& file_id)
  {
    while (engine->hasRead(file_id))
    {
      shared_ptr<Value> cur_val = engine->doReadExternal(file_id);
      if (cur_val)
        initial_accumulator = accumulation(initial_accumulator, *cur_val);
      else
        return initial_accumulator;
    }
    return initial_accumulator;
  }

  template<typename AccumT>
  AccumT foldFile(Engine * engine, std::function<AccumT(AccumT, Value)> accumulation, AccumT initial_accumulator, const Identifier& file_id)
  {
    openCollectionFile(engine, file_id, IOMode::Read);
    AccumT result = foldOpenFile<AccumT>(engine, accumulation, initial_accumulator, file_id);
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
        engine->doWriteExternal(new_id, *iter);
    }
    engine->close(new_id);
    return new_id;
  }


  shared_ptr<Value> peekFile(Engine * engine, const Identifier& file_id);

  Identifier mapFile(Engine * engine, std::function<Value(Value)> function, const Identifier& file_ds);
  void mapFile_(Engine * engine, std::function<void(Value)> function, const Identifier& file_id);

  Identifier filterFile(Engine * engine, std::function<bool(Value)> predicate, const Identifier& old_id);

  Identifier insertFile(Engine * engine, const Identifier& old_id, const Value& v);

  Identifier deleteFile(Engine * engine, const Identifier& old_id, const Value& v);

  Identifier updateFile(Engine * engine, const Value& old_val, const Value& new_val, const Identifier& file_id);

  Identifier combineFile(Engine * engine, const Identifier& self, const Identifier& values);

  tuple<Identifier, Identifier> splitFile(Engine * engine, const Identifier& file_id);

  typedef Value E;

  class FileDataspace
  {
    protected:
      Engine * engine;
      Identifier file_id;

      public:
      FileDataspace(Engine * eng)
          : engine(eng), file_id(generateCollectionFilename(eng))
      {
        emptyFile(eng);
      }

      template<typename Iterator>
      FileDataspace(Engine * eng, Iterator start, Iterator finish)
          : engine(eng), file_id(initialFile(eng, start, finish))
      { }

      FileDataspace(const FileDataspace& other)
          : engine(other.engine), file_id(copyFile(other.engine, other.file_id))
      { }

    private:
      FileDataspace(Engine * eng, Identifier f)
          : engine(eng), file_id(f)
      { }

    public:
      shared_ptr<E> peek() const
      {
        return peekFile(engine, file_id);
      }

      void insert_basic(const E& v)
      {
        file_id = insertFile(engine, file_id, v);
      }
      void delete_first(const E& v)
      {
        file_id = deleteFile(engine, file_id, v);
      }
      void update_first(const E& v, const E& v2)
      {
        file_id = updateFile(engine, v, v2, file_id);
      }

      template<typename Z>
      Z fold(std::function<Z(Z, E)> accum, Z initial_accumulator)
      {
        return foldFile<Z>(engine, accum, initial_accumulator, file_id);
      }

      //template<typename T>
      typedef Value T;
      FileDataspace map(std::function<T(E)> func)
      {
        return FileDataspace(engine,
                mapFile(engine, func, file_id)
                );
      }

      void iterate(std::function<void(E)> func)
      {
        mapFile_(engine, func, file_id);
      }

      FileDataspace filter(std::function<bool(E)> predicate)
      {
        return FileDataspace(engine,
                filterFile(engine, predicate, file_id)
                );
      }

      tuple< FileDataspace, FileDataspace > split()
      {
        tuple<Identifier, Identifier> halves = splitFile(engine, file_id);
        return make_tuple(
                FileDataspace(engine, get<0>(halves)),
                FileDataspace(engine, get<1>(halves))
                );
      }
      FileDataspace combine(FileDataspace other)
      {
        return FileDataspace(engine,
                combineFile(engine, file_id, other.file_id)
                );
      }
  };
};