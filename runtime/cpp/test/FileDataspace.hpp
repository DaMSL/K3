#include <algorithm>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include <Common.hpp>
#include <Engine.hpp>

template<typename T>
std::string toString(const T& not_string)
{
    std::ostringstream strm;
    strm << not_string;
    return strm.str();
}

template<>
std::string toString(const K3::Address& not_string);

template<typename T>
T fromString(std::string str)
{
    std::istringstream strm(str);
    T not_string;
    strm >> not_string;
    return not_string;
}

std::string generateListDataspaceFilename(K3::Engine * engine);
std::string openListDataspaceFile(K3::Engine * engine, const K3::Identifier& name, K3::IOMode mode);
K3::Identifier openCollectionFile(K3::Engine * engine, const K3::Identifier& name, K3::IOMode mode);

template<typename AccumT>
AccumT foldOpenFile(K3::Engine * engine, std::function<AccumT(AccumT, K3::Value)> accumulation, AccumT initial_accumulator, const K3::Identifier& file_id)
{
    while (engine->hasRead(file_id))
    {
        std::shared_ptr<K3::Value> cur_val = engine->doReadExternal(file_id);
        if (cur_val)
            initial_accumulator = accumulation(initial_accumulator, *cur_val);
        else
            return initial_accumulator;
    }
    return initial_accumulator;
}

template<typename AccumT>
AccumT foldFile(K3::Engine * engine, std::function<AccumT(AccumT, K3::Value)> accumulation, AccumT initial_accumulator, const K3::Identifier& file_id)
{
    openCollectionFile(engine, file_id, K3::IOMode::Read);
    AccumT result = foldOpenFile<AccumT>(engine, accumulation, initial_accumulator, file_id);
    engine->close(file_id);
    return result;
}

std::string copyFile(K3::Engine * engine, const std::string& old_id);

template<typename Iterator>
K3::Identifier initialFile(K3::Engine* engine, Iterator start, Iterator finish)
{
    K3::Identifier new_id = generateListDataspaceFilename(engine);
    openListDataspaceFile(engine, new_id, K3::IOMode::Write);
    for (Iterator iter = start; iter != finish; ++iter )
    {
        engine->doWriteExternal(new_id, *iter);
    }
    engine->close(new_id);
    return new_id;
}


std::shared_ptr<K3::Value> peekFile(K3::Engine * engine, const K3::Identifier& file_id);

K3::Identifier mapFile(K3::Engine * engine, std::function<K3::Value(K3::Value)> function, const K3::Identifier& file_ds);
void mapFile_(K3::Engine * engine, std::function<void(K3::Value)> function, const K3::Identifier& file_id);

K3::Identifier filterFile(K3::Engine * engine, std::function<bool(K3::Value)> predicate, const K3::Identifier& old_id);

K3::Identifier insertFile(K3::Engine * engine, const K3::Identifier& old_id, const K3::Value& v);

K3::Identifier deleteFile(K3::Engine * engine, const K3::Identifier& old_id, const K3::Value& v);

K3::Identifier updateFile(K3::Engine * engine, const K3::Value& old_val, const K3::Value& new_val, const K3::Identifier& file_id);

K3::Identifier combineFile(K3::Engine * engine, const K3::Identifier& self, const K3::Identifier& values);

std::tuple<K3::Identifier, K3::Identifier> splitFile(K3::Engine * engine, const K3::Identifier& file_id);

// template<typename E>
typedef K3::Value E;
class FileDataspace
{
    protected:
    K3::Engine * engine;
    K3::Identifier file_id;

    public:
    FileDataspace(K3::Engine * eng)
        : engine(eng), file_id(generateListDataspaceFilename(eng))
    { }

    template<typename Iterator>
    FileDataspace(K3::Engine * eng, Iterator start, Iterator finish)
        : engine(eng), file_id(initialFile(eng, start, finish))
    { }

    FileDataspace(const FileDataspace& other)
        : engine(other.engine), file_id(copyFile(other.engine, other.file_id))
    { }
    private:
    FileDataspace(K3::Engine * eng, K3::Identifier f)
        : engine(eng), file_id(f)
    { }

    public:
    std::shared_ptr<E> peek() const
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
    typedef K3::Value T;
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

    std::tuple< FileDataspace, FileDataspace > split()
    {
        std::tuple<K3::Identifier, K3::Identifier> halves = splitFile(engine, file_id);
        return std::make_tuple(
                FileDataspace(engine, std::get<0>(halves)),
                FileDataspace(engine, std::get<1>(halves))
                );
    }
    FileDataspace combine(FileDataspace other)
    {
        return FileDataspace(engine,
                combineFile(engine, file_id, other.file_id)
                );
    }
};
class ListDataspace
{
    using chunk = std::list<E>;

    protected:
    chunk __data;

    public:
    ListDataspace(K3::Engine *)
    { }

    template<typename Iterator>
    ListDataspace(K3::Engine *, Iterator start, Iterator finish)
        : __data(start, finish)
    { }

    ListDataspace(const ListDataspace& other)
        : __data(other.__data)
    { }
    private:
    ListDataspace(std::list<E> other)
        : __data(other)
    { }

    public:
    std::shared_ptr<E> peek() const
    {
        if (__data.empty())
            return std::make_shared<E>(nullptr);
        else
            return std::make_shared<E>(__data.front());
    }

    void insert_basic(const E& e)
    {
        __data.emplace_back(e);
    }
    void delete_first(const E& e)
    {
        auto location = std::find(begin(__data), end(__data), e);

        if (location != end(__data)) {
            __data.erase(location);
        }
    }
    void update_first(const E& prev, const E& next) {
        auto location = find(begin(__data), end(__data), prev);

        if (location != end(__data)) {
            *location = next;
        }

        return;
    }

    std::tuple<ListDataspace, ListDataspace> split() {
        if (__data.size() < 2) {
            // First of the pair is a copy of the original collection, the second is empty.
            return std::make_tuple(ListDataspace(*this), ListDataspace(chunk()));
        } else {
            typename chunk::iterator s = begin(__data);
            for (unsigned i = 0; i < __data.size()/2; ++i, ++s);

            chunk left(begin(__data), s);
            chunk right(s, end(__data));

            return std::make_tuple(ListDataspace(left), ListDataspace(right));
        }
    }

    ListDataspace combine(ListDataspace other)
    {
        chunk result;

        result.insert(end(result), begin(__data), end(__data));
        result.insert(end(result), begin(other.__data), end(other.__data));

        return result;
    }

    void iterate(std::function<void(E)> f) {
        for (auto i: __data) {
            f(i);
        }

        return;
    }

    //template <typename T>
    typedef K3::Value T;
    ListDataspace map(std::function<T(E)> f) {
        chunk v;
        //v.reserve(__data.size());

        for (auto i : __data) {
            v.push_back(f(i));
        }

        return ListDataspace(v);
    }

    //template <typename E>
    typedef K3::Value E;
    ListDataspace filter(std::function<bool(E)> p) {
        chunk v;

        for (auto i: __data) {
            if (p(i)) {
                v.push_back(i);
            }
        }

        return ListDataspace(v);
    }

    //template <typename E>
    template <typename Z>
    Z fold(std::function<Z(Z, E)> f, Z z) {
        for (auto i: __data) {
            z = f(z, i);
        }

        return z;
    }
};
