#include <iostream>
#include <sstream>
#include <string>
#include <runtime/cpp/IOHandle.hpp>
#include <runtime/cpp/Endpoint.hpp>
#include <boost/algorithm/string.hpp>
#include <xUnit++.h>

using namespace K3;
// Utils

void do_nothing(const Address&, const Identifier& , const string&)
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

// Begin Tests
FACT("FileHandle read file by line")
{
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  shared_ptr<FileHandle<string>> f = make_shared<FileHandle<string>>(wd, path, LineBasedHandle<string>::Input());
  string actual = readFile<FileHandle<string>>(f);  
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}


FACT("Endpoint read file by line. ScalarST Buffer")
{
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  auto f = make_shared<FileHandle<string>>(wd, path, LineBasedHandle<string>::Input());
  
  // Setup a K3 Endpoint
  auto buf = make_shared<ScalarEPBufferST<string>>();
  EndpointBindings<string>::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings<string>>(func);
  
  Endpoint<string, string> ep(f, buf, bindings);
  string actual = readFile<Endpoint<string, string>>(make_shared<Endpoint<string,string>>(ep));
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}

FACT("Endpoint read file by line. ScalarMT Buffer")
{
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  auto f = make_shared<FileHandle<string>>(wd, path, LineBasedHandle<string>::Input());
  
  // Setup a K3 Endpoint
  auto buf = make_shared<ScalarEPBufferMT<string>>();
  EndpointBindings<string>::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings<string>>(func);
  
  Endpoint<string, string> ep(f, buf, bindings);
  string actual = readFile<Endpoint<string, string>>(make_shared<Endpoint<string,string>>(ep));
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}

FACT("Endpoint read file by line. ContainerST Buffer")
{
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  auto f = make_shared<FileHandle<string>>(wd, path, LineBasedHandle<string>::Input());
  
  // Setup a K3 Endpoint
  auto spec = BufferSpec(1000,100);
  auto buf = make_shared<ContainerEPBufferST<string>>(spec);
  EndpointBindings<string>::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings<string>>(func);
  
  Endpoint<string, string> ep(f, buf, bindings);

  string actual = readFile<Endpoint<string, string>>(make_shared<Endpoint<string,string>>(ep));
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}

FACT("Endpoint read file by line. ContainerMT Buffer")
{
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  auto f = make_shared<FileHandle<string>>(wd, path, LineBasedHandle<string>::Input());
  
  // Setup a K3 Endpoint
  auto spec = BufferSpec(1000,100);
  auto buf = make_shared<ContainerEPBufferMT<string>>(spec);
  EndpointBindings<string>::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings<string>>(func);
  
  Endpoint<string, string> ep(f, buf, bindings);
  

  string actual = readFile<Endpoint<string, string>>(make_shared<Endpoint<string,string>>(ep));
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}

