#include <iostream>
#include <sstream>
#include <string>
#include <runtime/cpp/IOHandle.hpp>
#include <runtime/cpp/Endpoint.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/iostreams/device/file.hpp>
#include <xUnit++/xUnit++.h>

using namespace K3;
// Utils

void do_nothing(const Address&, const Identifier& , shared_ptr<Value>)
{
}
// Test Utils
template <class R>
string readFile(shared_ptr<R> r) {
  // Read the file line-by-line
  ostringstream os;
  while (r->hasRead()) {
    shared_ptr<string> sP = r->doRead();
    // Replace the trailing newline with a space
    string s = *sP;
    boost::algorithm::replace_all(s,"\n"," ");
    os << s;
  }
  string actual = os.str();
  return actual;
}

shared_ptr<FileHandle> createFileHandle(string path) {
  // Setup a K3 Codec 
  shared_ptr<DefaultCodec> cdec = shared_ptr<DefaultCodec>(new DefaultCodec());
  auto x = LineBasedHandle::Input();
  // Create a file source
  file_source fs = file_source(path); 
  // Construct File Handle
  shared_ptr<FileHandle> f = shared_ptr<FileHandle>(new FileHandle(cdec, fs, x));
  return f;
}

// Begin Tests
FACT("File handle reads file by line")
{
  // Define input path
  string path = "in.txt";
  // Create a File Handle
  shared_ptr<FileHandle> f = createFileHandle(path);
  // read from file
  string actual = readFile<FileHandle>(f);  
  // compare results
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}

FACT("Endpoint read file by line. ScalarST Buffer")
{
  // Define input path
  string path = "in.txt";
  // Create FileHandle
  shared_ptr<FileHandle> f = createFileHandle(path);

  // Setup a K3 Endpoint
  auto buf = make_shared<ScalarEPBufferST>();
  EndpointBindings::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings, nullptr));
  string actual = readFile<Endpoint>(ep);
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}

FACT("Endpoint read file by line. ContainerST Buffer")
{
  // Define input path
  string path = "in.txt";
  // Create FileHandle
  shared_ptr<FileHandle> f = createFileHandle(path);

  // Setup a K3 Endpoint

  auto buf = make_shared<ContainerEPBufferST>(BufferSpec(100,10));
  EndpointBindings::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings, nullptr));
  string actual = readFile<Endpoint>(ep);
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}
