#include <algorithm>

//#include "xUnit++/xUnit++.h"

#include "FileDataspace.hpp"

template<typename DS>
bool containsDS(DS ds, const K3::Value& val)
{
    return ds.template fold<bool>(
            [val](bool fnd, K3::Value cur) {
                if (fnd || val == cur)
                    return true;
                else
                    return fnd;
            }, false);
}

template<typename DS>
unsigned sizeDS(DS ds)
{
    return ds.template fold<unsigned>(
            [](unsigned count, K3::Value) {
                return count + 1;
            }, 0);
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
std::shared_ptr<DS> findAndRemoveElement(std::shared_ptr<DS> ds, const K3::Value& val)
{
    if (!ds)
        return std::shared_ptr<DS>(nullptr);
    else {
        if (containsDS(*ds, val)) {
            ds->delete_first(val);
            return ds;
        }
        else
            throw ElementNotFoundException(val);
    }
}

template<typename DS>
bool compareDataspaceToList(DS dataspace, std::vector<K3::Value> l)
{
    std::shared_ptr<DS> ds_ptr = std::make_shared<DS>(dataspace);
    std::shared_ptr<DS> result = std::accumulate(begin(l), end(l), ds_ptr,
            [](std::shared_ptr<DS> ds, K3::Value cur_val) {
                return findAndRemoveElement(ds, cur_val);
            });
    if (result) {
        auto s = sizeDS(*result);
        if (s == 0)
            return true;
        else
            throw ExtraElementsException(s);
    }
    else
        return false;
}

template<typename DS>
bool emptyPeek(std::shared_ptr<K3::Engine> engine)
{
    DS d(engine.get());
    std::shared_ptr<K3::Value> result = d.peek();
    return result == nullptr;
}

template<typename DS>
bool testEmptyFold(std::shared_ptr<K3::Engine> engine)
{
    DS d(engine.get());
    unsigned counter = d.template fold<unsigned>(
            [](unsigned accum, K3::Value) {
                return accum + 1;
            }, 0);
    return counter == 0;
}

const std::vector<K3::Value> test_lst({"1", "2", "3", "4", "4", "100"});

template<typename DS>
bool testPeek(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    std::shared_ptr<K3::Value> peekResult = test_ds.peek();
    if (!peekResult)
        throw std::runtime_error("Peek returned nothing!");
    else
        return containsDS(test_ds, *peekResult);
}

template<typename DS>
bool testInsert(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get());
    for( K3::Value val : test_lst )
        test_ds.insert_basic(val);
    return compareDataspaceToList(test_ds, test_lst);
}

template<typename DS>
bool testDelete(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    test_ds.delete_first("3");
    test_ds.delete_first("4");
    std::vector<K3::Value> deleted_lst({"1", "2", "4", "100"});
    return compareDataspaceToList(test_ds, deleted_lst);
}

template<typename DS>
bool testMissingDelete(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    test_ds.delete_first("5");
    return compareDataspaceToList(test_ds, test_lst);
}

template<typename DS>
bool testUpdate(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    test_ds.update_first("1", "4");
    std::vector<K3::Value> updated_answer({"4", "2", "3", "4", "4", "100"});
    return compareDataspaceToList(test_ds, updated_answer);
}

template<typename DS>
bool testUpdateMultiple(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    test_ds.update_first("4", "5");
    std::vector<K3::Value> updated_answer({"1", "2", "3", "5", "4", "100"});
    return compareDataspaceToList(test_ds, updated_answer);
}

template<typename DS>
bool testUpdateMissing(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    test_ds.update_first("40", "5");
    std::vector<K3::Value> updated_answer({"1", "2", "3", "4", "4", "100", "5"});
    return compareDataspaceToList(test_ds, updated_answer);
}

template<typename DS>
bool testFold(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    unsigned test_sum = test_ds.template fold<unsigned>(
            [](unsigned sum, K3::Value val) {
                sum += fromString<unsigned>(val);
                return sum;
            }, 0);
    return test_sum == 114;
}

template<typename DS>
bool testMap(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    DS mapped_ds = test_ds.map(
            [](K3::Value val) {
                return toString(fromString<int>(val) + 5);
            });
    std::vector<K3::Value> mapped_answer({"6", "7", "8", "9", "9", "105"});
    return compareDataspaceToList(mapped_ds, mapped_answer);
}

template<typename DS>
bool testFilter(std::shared_ptr<K3::Engine> engine)
{
    DS test_ds(engine.get(), begin(test_lst), end(test_lst));
    DS filtered = test_ds.filter([](K3::Value val) {
            return fromString<int>(val) > 50;
            });
    std::vector<K3::Value> filtered_answer({"100"});
    return compareDataspaceToList(filtered, filtered_answer);
}

template<typename DS>
bool testCombine(std::shared_ptr<K3::Engine> engine)
{
    DS left_ds(engine.get(), begin(test_lst), end(test_lst));
    DS right_ds(engine.get(), begin(test_lst), end(test_lst));
    DS combined = left_ds.combine(right_ds);

    std::vector<K3::Value> long_lst(test_lst);
    long_lst.insert(end(long_lst), begin(test_lst), end(test_lst));
    return compareDataspaceToList(combined, long_lst);
}

template<typename DS>
bool testCombineSelf(std::shared_ptr<K3::Engine> engine)
{
    DS self(engine.get(), begin(test_lst), end(test_lst));
    DS combined = self.combine(self);

    std::vector<K3::Value> long_lst(test_lst);
    long_lst.insert(end(long_lst), begin(test_lst), end(test_lst));
    return compareDataspaceToList(combined, long_lst);
}

template<typename DS>
std::shared_ptr<std::tuple<DS, DS>> split_findAndRemoveElement(std::shared_ptr<std::tuple<DS, DS>> maybeTuple, K3::Value cur_val)
{
    typedef std::shared_ptr<std::tuple<DS, DS>> MaybePair;
    if (!maybeTuple) {
        return nullptr;
    }
    else {
        DS& left = std::get<0>(*maybeTuple);
        DS& right = std::get<1>(*maybeTuple);
        if (containsDS(left, cur_val)) {
            left.delete_first(cur_val);
            // TODO copying everywhere!
            // this copies the DS into the tuple, then the tuple is copied into the new target of the shared_ptr
            return std::make_shared<std::tuple<DS, DS>>(std::make_tuple(left, right));
        }
        else if (containsDS(right, cur_val)) {
            right.delete_first(cur_val);
            // TODO see above
            return std::make_shared<std::tuple<DS, DS>>(std::make_tuple(left, right));
        }
        else {
            throw ElementNotFoundException(cur_val);
            return MaybePair(nullptr);
        }
    }
}

template<typename DS>
bool testSplit(std::shared_ptr<K3::Engine> engine)
{
    std::vector<K3::Value> long_lst(test_lst);
    long_lst.insert(end(long_lst), begin(test_lst), end(test_lst));
    DS first_ds(engine.get(), begin(long_lst), end(long_lst));
    std::tuple<DS, DS> split_pair = first_ds.split();
    DS& left = std::get<0>(split_pair);
    DS& right = std::get<1>(split_pair);

    unsigned leftLen = sizeDS(left);
    unsigned rightLen = sizeDS(right);

    // TODO should the >= be just > ?
    if (leftLen >= long_lst.size() || rightLen >= long_lst.size() || leftLen + rightLen > long_lst.size())
        return false;
    else {
        std::shared_ptr<std::tuple<DS, DS>> remainders = std::make_shared<std::tuple<DS, DS>>(std::make_tuple(left, right));
        for (K3::Value val : long_lst)
            remainders = split_findAndRemoveElement(remainders, val);
        if (!remainders) {
            return false;
        }
        else {
            const DS& l = std::get<0>(*remainders);
            const DS& r = std::get<1>(*remainders);
            return (sizeDS(l) == 0 && sizeDS(r) == 0);
        }
    }
}

// TODO This just makes sure that nothing crashes, but should probably check for correctness also
// This test is commented out because it segfaults clang 3.4.  It's bug #18473.
// There's a temporary fix in r200954 that may be included in 3.4.1, but that's not released yet.
//template<typename DS>
//bool insertInsideMap(std::shared_ptr<K3::Engine> engine)
//{
//    DS outer_ds(engine.get(), begin(test_lst), end(test_lst));
//    DS result_ds = outer_ds.map([&outer_ds, &v, &v4](K3::Value cur_val) {
//            outer_ds.insert_basic("256");
//            return "4";
//            });
//    return true;
//}

// Engine setup
std::shared_ptr<K3::Engine> buildEngine()
{
  // Configure engine components
  bool simulation = true;
  K3::SystemEnvironment s_env = K3::defaultEnvironment();
  auto i_cdec = std::shared_ptr<K3::InternalCodec>(new K3::DefaultInternalCodec());

  // Construct an engine
  K3::Engine * engine = new K3::Engine(simulation, s_env, i_cdec);
  return std::shared_ptr<K3::Engine>(engine);
}

void callTest(std::function<bool(std::shared_ptr<K3::Engine>)> testFunc)
{
    auto engine = buildEngine();
    bool success = false;
    try {
       success = testFunc(engine);
       assert(success == true);
       //xUnitpp::Assert.Equal(success, true);
    }
    catch( std::exception& e)
    {
        std::cerr << e.what();
        assert(false);
        //xUnitpp::Assert.Fail() << e.what();
    }
}

//#define MAKE_TEST(name, function, ds) \
//    FACT(name) { callTest(function<ds>); }
#define MAKE_TEST(name, function, ds) \
    callTest(function<ds>);


//SUITE("List Dataspace") {
//    MAKE_TEST( "EmptyPeek", emptyPeek, ListDataspace)
//    MAKE_TEST( "Fold on Empty List Test", testEmptyFold, ListDataspace)
//    MAKE_TEST( "Peek Test", testPeek, ListDataspace)
//    MAKE_TEST( "Insert Test", testInsert, ListDataspace)
//    MAKE_TEST( "Delete Test", testDelete, ListDataspace)
//    MAKE_TEST( "Delete of missing element Test", testMissingDelete, ListDataspace)
//    MAKE_TEST( "Update Test", testUpdate, ListDataspace)
//    MAKE_TEST( "Update Multiple Test", testUpdateMultiple, ListDataspace)
//    MAKE_TEST( "Update missing element Test", testUpdateMissing, ListDataspace)
//    MAKE_TEST( "Fold Test", testFold, ListDataspace)
//    MAKE_TEST( "Map Test", testMap, ListDataspace)
//    MAKE_TEST( "Filter Test", testFilter, ListDataspace)
//    MAKE_TEST( "Combine Test", testCombine, ListDataspace)
//    MAKE_TEST( "Combine with Self Test", testCombineSelf, ListDataspace)
//    MAKE_TEST( "Split Test", testSplit, ListDataspace)
//    MAKE_TEST( "Insert inside map", insertInsideMap, ListDataspace)
//}

//SUITE("File Dataspace") {
int main()
{
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
    //MAKE_TEST( "Insert inside map", insertInsideMap, FileDataspace)
}

//MAKE_TEST_GROUP("List Dataspace", ListDataspace)
//MAKE_TEST_GROUP("File Dataspace", FileDataspace)
