#include <string>
#include <boost/filesystem.hpp>
#include "FileDataspace.hpp"

static const boost::filesystem::path rootDataPath = "__DATA";

boost::filesystem::path engineDataPath(K3::Engine * engine)
{
    std::string addrString = K3::addressAsString(engine->getAddress());
    return (rootDataPath / addrString);
}

void createDataDir(const boost::filesystem::path& path)
{
    bool dirExists = boost::filesystem::is_directory(path);
    if (!dirExists) {
        if (boost::filesystem::exists(path)) {
            throw std::runtime_error(path.string() + " exists but is not a directory, so it cannot be used to store external collections.");
        }
        else {
            boost::filesystem::create_directory(path);
        }
    }
}

static void initDataDir(K3::Engine * engine)
{
    createDataDir(rootDataPath);
    createDataDir(engineDataPath(engine));
}

// TODO use boost::filesystem::path?
K3::Identifier generateCollectionFilename(K3::Engine * engine)
{
    initDataDir(engine);
    unsigned counter = engine->getCollectionCount();
    K3::Identifier filename = "collection_" + toString(counter);
    engine->incrementCollectionCount();
    return (engineDataPath(engine) / filename).string();
}

K3::Identifier openCollectionFile(K3::Engine * engine, const K3::Identifier& name, K3::IOMode mode)
{
    engine->openFile(name, name, engine->getExternalFormat(), mode);
    return name;
}

K3::Identifier emptyFile(K3::Engine * engine)
{
    K3::Identifier file_id = generateCollectionFilename(engine);
    openCollectionFile(engine, file_id, K3::IOMode::Write);
    engine->close(file_id);
    return file_id;
}

//template<typename AccumT>
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

AccumT foldFile(K3::Engine * engine, std::function<AccumT(AccumT, K3::Value)> accumulation, AccumT initial_accumulator, const K3::Identifier& file_id)
{
    openCollectionFile(engine, file_id, K3::IOMode::Read);
    AccumT result = foldOpenFile(engine, accumulation, initial_accumulator, file_id);
    engine->close(file_id);
    return result;
}

std::shared_ptr<K3::Value> peekFile(K3::Engine * engine, const K3::Identifier& file_id)
{
    openCollectionFile(engine, file_id, K3::IOMode::Read);
    if (!engine->hasRead(file_id))
        return std::shared_ptr<K3::Value>(nullptr);
    std::shared_ptr<K3::Value> result = engine->doReadExternal(file_id);
    engine->close(file_id);
    return result;
}

K3::Identifier mapFile(K3::Engine * engine, std::function<K3::Value(K3::Value)> function, const K3::Identifier& file_ds)
{
    K3::Identifier tmp_ds = copyFile(engine, file_ds);
    K3::Identifier new_id = generateCollectionFilename(engine);
    openCollectionFile(engine, new_id, K3::IOMode::Write);
    foldFile(engine,
            [engine, &new_id, &function](K3::Value, K3::Value val) {
                engine->doWriteExternal(new_id, function(val));
                return K3::Value();
            }, K3::Value(), tmp_ds);
    engine->close(new_id);
    return new_id;
}
void mapFile_(K3::Engine * engine, std::function<void(K3::Value)> function, const K3::Identifier& file_ds)
{
    K3::Identifier tmp_ds = copyFile(engine, file_ds);
    foldFile(engine,
            [engine, &function](K3::Value, K3::Value val) {
                function(val);
                return K3::Value();
            }, K3::Value(), tmp_ds);
}

K3::Identifier filterFile(K3::Engine * engine, std::function<bool(K3::Value)> predicate, const K3::Identifier& old_ds)
{
    K3::Identifier tmp_ds = copyFile(engine, old_ds);
    K3::Identifier new_id = generateCollectionFilename(engine);
    openCollectionFile(engine, new_id, K3::IOMode::Write);
    foldFile(engine,
            [engine, &new_id, &predicate](K3::Value, K3::Value val) {
                if (predicate(val))
                    engine->doWriteExternal(new_id, val);
                return K3::Value();
            }, K3::Value(), tmp_ds);
    engine->close(new_id);
    return new_id;
}

K3::Identifier insertFile(K3::Engine * engine, const K3::Identifier& old_id, const K3::Value& v)
{
    openCollectionFile(engine, old_id, K3::IOMode::Append);
    engine->doWriteExternal(old_id, v);
    engine->close(old_id);
    return old_id;
}

K3::Identifier deleteFile(K3::Engine * engine, const K3::Identifier& old_id, const K3::Value& v)
{
    K3::Identifier deleted_id = generateCollectionFilename(engine);
    openCollectionFile(engine, deleted_id, K3::IOMode::Write);
    foldFile(engine,
            [engine, &deleted_id, v](K3::Value found_str, K3::Value val) {
                if (!found_str.size() && val == v)
                    return K3::Value("true");
                else {
                    engine->doWriteExternal(deleted_id, val);
                    return found_str;
                }
            }, K3::Value(), old_id);
    engine->close(deleted_id);
    boost::filesystem::rename(deleted_id, old_id);
    return old_id;
}

K3::Identifier updateFile(K3::Engine * engine, const K3::Value& old_val, const K3::Value& new_val, const K3::Identifier& file_id)
{
    K3::Identifier new_id = generateCollectionFilename(engine);
    openCollectionFile(engine, new_id, K3::IOMode::Write);
    K3::Identifier did_update = foldFile(engine,
            [engine, &new_id, old_val, new_val](K3::Value found_str, K3::Value val) {
                if (!found_str.size() && val == old_val) {
                    engine->doWriteExternal(new_id, new_val);
                    return K3::Value("true");
                }
                else {
                    engine->doWriteExternal(new_id, val);
                    return found_str;
                }
            }, K3::Value(), file_id);
    if (!did_update.size())
        engine->doWriteExternal(new_id, new_val);
    engine->close(new_id);
    boost::filesystem::rename(new_id, file_id);
    return file_id;
}

K3::Identifier combineFile(K3::Engine * engine, const K3::Identifier& self, const K3::Identifier& values)
{
    K3::Identifier new_id = copyFile(engine, self);
    openCollectionFile(engine, new_id, K3::IOMode::Append);
    foldFile(engine,
            [engine, &new_id](K3::Value, K3::Value v) {
                engine->doWriteExternal(new_id, v);
                return K3::Value();
            }, K3::Value(), values);
    engine->close(new_id);
    return new_id;
}

std::tuple<K3::Identifier, K3::Identifier> splitFile(K3::Engine * engine, const K3::Identifier& file_id)
{
    K3::Identifier left = openCollectionFile(engine, generateCollectionFilename(engine), K3::IOMode::Write);
    K3::Identifier right = openCollectionFile(engine, generateCollectionFilename(engine), K3::IOMode::Write);
    foldFile(engine,
            [engine, &left, &right](K3::Value file, K3::Value cur_val) {
                if (file == "left") {
                    engine->doWriteExternal(left, cur_val);
                    return K3::Value("right");
                }
                else {
                    engine->doWriteExternal(right, cur_val);
                    return K3::Value("left");
                }
            }, K3::Value("left"), file_id);
    engine->close(left);
    engine->close(right);
    return std::make_tuple(left, right);
}
