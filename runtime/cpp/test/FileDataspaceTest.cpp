#include <algorithm>

#include "xUnit++/xUnit++.h"

#include "FileDataspace.hpp"

template<typename DS>
bool containsDS(DS ds, const K3::Value& val)
{
    return ds.fold(
            [val](K3::Value fnd, K3::Value cur) {
                if (val == cur)
                    return K3::Value("found it!");
                else
                    return fnd;
            }, K3::Value()).size() > 0;
}

template<typename DS>
unsigned sizeDS(DS ds)
{
    return fromString<unsigned>(ds.fold(
            [](K3::Value str_count, K3::Value) {
                return toString(fromString<unsigned>(str_count) + 1);
            }, K3::Value("0")));
}

class ElementNotFoundException : public std::runtime_error
{
    public:
    ElementNotFoundException(const K3::Value& val)
        : std::runtime_error("Could not find element " + val)
    {}
};
class ExtraElementsException : public std::runtime_error
{
    public:
    ExtraElementsException(unsigned i)
        : std::runtime_error("Dataspace had " + toString(i) + " extra elements")
    { }
};

template<typename DS>
DS findAndRemoveElement(std::shared_ptr<K3::Engine> engine, DS ds, const K3::Value& val)
{
    if (!ds)
        return std::make_shared<DS>(nullptr);
    else {
        if (containsDS(engine, *ds, val))
            return std::make_shared(deleteDS(engine, ds, val));
        else
            throw ElementNotFoundException(val);
    }
}

template<typename DS>
bool compareDataspaceToList(std::shared_ptr<K3::Engine> engine, DS dataspace, std::vector<K3::Value> l)
{
    K3::Value result = foldDS(engine,
            [engine](K3::Value, DS ds, K3::Value cur_val) {
                // TODO how does ownership of the dataspace work with the make_shared?
                return findAndRemoveElement(engine, ds, cur_val);
            }, dataspace, l);
    auto s = sizeDS(result);
    if (s == 0)
        return true;
    else
        throw ExtraElementsException(s);
}

template<typename DS>
bool emptyPeek(std::shared_ptr<K3::Engine> engine)
{
    DS d(engine);
    std::shared_ptr<K3::Value> result = d.peek();
    return result == nullptr;
}

template<typename DS>
bool testEmptyFold(std::shared_ptr<K3::Engine> engine)
{
    DS d(engine);
    K3::Value counter = d.fold(
            [](K3::Value accum, K3::Value) {
                return toString(fromString<int>(accum)+1);
            }, K3::Value("0"));
    return counter == "0";
}

const std::vector<K3::Value> test_lst({"1", "2", "3", "4", "4", "100"});

template<typename DS>
bool testPeek(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine, begin(test_lst), end(test_lst));
    std::shared_ptr<K3::Value> peekResult = peekDS(engine, test_ds);
    if (peekResult)
        throw std::runtime_error("Peek returned nothing!");
    else
        return containsDS(engine, test_ds, *peekResult);
}

template<typename DS>
bool testInsert(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine);
}

// Engine setup
std::shared_ptr<K3::Engine> buildEngine()
{
  // Configure engine components
  bool simulation = true;
  K3::SystemEnvironment s_env = K3::defaultEnvironment();
  auto i_cdec = std::shared_ptr<K3::InternalCodec>(new K3::DefaultInternalCodec());
  auto e_cdec = std::shared_ptr<K3::ExternalCodec>(new K3::DefaultCodec());

  // Construct an engine
  K3::Engine * engine = new K3::Engine(simulation, s_env, i_cdec, e_cdec);
  return std::shared_ptr<K3::Engine>(engine);
}

void callTest(std::function<bool(std::shared_ptr<K3::Engine>)> testFunc)
{
    auto engine = buildEngine();
    bool success = false;
    try {
       success = testFunc(engine);
       xUnitpp::Assert.Equal(success, true);
    }
    catch( std::exception e)
    {
        xUnitpp::Assert.Fail() << e.what();
    }
}

#define MAKE_TEST(name, function, ds) \
    FACT(name) { callTest(function<ds>); }

SUITE("List Dataspace") {
    MAKE_TEST( "EmptyPeek", emptyPeek, ListDataspace)
    MAKE_TEST( "Fold on Empty List Test", testEmptyFold, ListDataspace)
    MAKE_TEST( "Peek Test", testPeek, ListDataspace)
    MAKE_TEST( "Insert Test", testInsert, ListDataspace)
    MAKE_TEST( "Delete Test", testDelete, ListDataspace)
    MAKE_TEST( "Delete of missing element Test", testMissingDelete, ListDataspace)
    MAKE_TEST( "Update Test", testUpdate, ListDataspace)
    MAKE_TEST( "Update Multiple Test", testUpdateMultiple, ListDataspace)
    MAKE_TEST( "Update missing element Test", testUpdateMissing, ListDataspace)
    MAKE_TEST( "Fold Test", testFold, ListDataspace)
    MAKE_TEST( "Map Test", testMap, ListDataspace)
    MAKE_TEST( "Filter Test", testFilter, ListDataspace)
    MAKE_TEST( "Combine Test", testCombine, ListDataspace)
    MAKE_TEST( "Combine with Self Test", testCombineSelf, ListDataspace)
    MAKE_TEST( "Split Test", testSplit, ListDataspace)
    MAKE_TEST( "Insert inside map", insertInsideMap, ListDataspace)
}

SUITE("File Dataspace") {
    MAKE_TEST( "EmptyPeek", emptyPeek, FileDataspace)
    MAKE_TEST( "Fold on Empty List Test", testEmptyFold, FileDataspace)
    MAKE_TEST( "Peek Test", testPeek, FileDataspace)
    MAKE_TEST( "Insert Test", testInsert, FileDataspace)
    MAKE_TEST( "Delete Test", testDelete, FileDataspace)
    MAKE_TEST( "Delete of missing element Test", testMissingDelete, FileDataspace)
    MAKE_TEST( "Update Test", testUpdate, FileDataspace)
    MAKE_TEST( "Update Multiple Test", testUpdateMultiple, FileDataspace)
    MAKE_TEST( "Update missing element Test", testUpdateMissing, FileDataspace)
    MAKE_TEST( "Fold Test", testFold, FileDataspace)
    MAKE_TEST( "Map Test", testMap, FileDataspace)
    MAKE_TEST( "Filter Test", testFilter, FileDataspace)
    MAKE_TEST( "Combine Test", testCombine, FileDataspace)
    MAKE_TEST( "Combine with Self Test", testCombineSelf, FileDataspace)
    MAKE_TEST( "Split Test", testSplit, FileDataspace)
    MAKE_TEST( "Insert inside map", insertInsideMap, FileDataspace)
}

//MAKE_TEST_GROUP("List Dataspace", ListDataspace)
//MAKE_TEST_GROUP("File Dataspace", FileDataspace)
