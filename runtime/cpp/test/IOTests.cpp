#include <iostream>
#include <sstream>
#include <string>
#include <memory>
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
tuple<int,string> readFile(shared_ptr<R> r) {
  // Read the file line-by-line
  int count = 0;
  ostringstream os;
  while (r->hasRead()) {
    count += 1;
    shared_ptr<string> sP = r->doRead();
    // Replace the trailing newline with a space
    string s = *sP;
    boost::algorithm::replace_all(s,"\n"," ");
    os << s;
  }
  string actual = os.str();
  return make_tuple(count,actual);
}

shared_ptr<FileHandle> createReadFileHandle(string path) {
  // Setup a K3 Codec 
  shared_ptr<DefaultCodec> cdec = shared_ptr<DefaultCodec>(new DefaultCodec());
  auto x = LineBasedHandle::Input();
  // Create a file source
  file_source fs = file_source(path); 
  // Construct File Handle
  shared_ptr<FileHandle> f = shared_ptr<FileHandle>(new FileHandle(cdec, fs, x));
  return f;
}

shared_ptr<FileHandle> createWriteFileHandle(string path) {
  // Setup a K3 Codec 
  shared_ptr<DefaultCodec> cdec = shared_ptr<DefaultCodec>(new DefaultCodec());
  auto x = LineBasedHandle::Output();
  // Create a file source
  file_sink fs = file_sink(path); 
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
  shared_ptr<FileHandle> f = createReadFileHandle(path);
  // read from file
  tuple<int,string> res = readFile<FileHandle>(f);  
  string actual = get<1>(res);
  int c = get<0>(res);
  // compare results
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
  Assert.Equal(c, 4);
}

FACT("File handle writes file by line")
{
  // Define input path
  string path = "out.txt";
  // Create a File Handle
  shared_ptr<FileHandle> f = createWriteFileHandle(path);
  // read from file
  string s0 = "Testing";
  string s1= "1";
  string s2 = "2";
  string s3 = "3";

  f->doWrite(s0);
  f->doWrite(s1);
  f->doWrite(s2);
  f->doWrite(s3); 
  f->close();
  shared_ptr<FileHandle> f2 = createReadFileHandle(path);

  tuple<int,string> res = readFile<FileHandle>(f2);  
  string actual = get<1>(res);
  int c = get<0>(res);
  // compare results
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
  Assert.Equal(c, 4);
}

FACT("Endpoint read file by line. ScalarST Buffer")
{
  // Define input path
  string path = "in.txt";
  // Create FileHandle
  shared_ptr<FileHandle> f = createReadFileHandle(path);

  // Setup a K3 Endpoint
  auto buf = make_shared<ScalarEPBufferST>();
  EndpointBindings::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings, nullptr));

  tuple<int,string> res = readFile<Endpoint>(ep);  
  string actual = get<1>(res);
  int c = get<0>(res);
  // compare results
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
  Assert.Equal(c, 4);
}

FACT("Endpoint read file by line. ContainerST Buffer")
{
  // Define input path
  string path = "in.txt";
  // Create FileHandle
  shared_ptr<FileHandle> f = createReadFileHandle(path);

  // Setup a K3 Endpoint

  auto buf = make_shared<ContainerEPBufferST>(BufferSpec(100,10));
  EndpointBindings::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings, nullptr));

  tuple<int,string> res = readFile<Endpoint>(ep);  
  string actual = get<1>(res);
  int c = get<0>(res);
  // compare results
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
  Assert.Equal(c, 4);
}
