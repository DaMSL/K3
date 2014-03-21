#include <iostream>
#include <sstream>
#include <string>
#include <memory>
#include <IOHandle.hpp>
#include <Endpoint.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/iostreams/device/file.hpp>
#include <xUnit++/xUnit++.h>

namespace K3 {

// Utils
void do_nothing(const Address&, const Identifier&, shared_ptr<Value>) {}

// Test Utils
template <class R>
tuple<int,string> readFile(K3::shared_ptr<R> r) {
  // Read the file line-by-line
  int count = 0;
  ostringstream os;
  while (r->hasRead()) {
    shared_ptr<string> val = r->doRead();
    if ( val ) {
      count += 1;
      os << *val;
    }
  }
  string actual = os.str();
  return make_tuple(count,actual);
}

shared_ptr<FileHandle> createReadFileHandle(string path) {
  // Setup a K3 Codec 
  shared_ptr<DelimiterCodec> cdec = shared_ptr<DelimiterCodec>(new DelimiterCodec('\n'));
  auto x = StreamHandle::Input();
  // Create a file source
  shared_ptr<file_source> fs = shared_ptr<file_source>(new file_source(path)); 
  // Construct File Handle
  shared_ptr<FileHandle> f = shared_ptr<FileHandle>(new FileHandle(cdec, fs, x));
  return f;
}

shared_ptr<FileHandle> createWriteFileHandle(string path) {
  // Setup a K3 Codec 
  shared_ptr<DelimiterCodec> cdec = shared_ptr<DelimiterCodec>(new DelimiterCodec('\n'));
  auto x = StreamHandle::Output();
  // Create a file source
  shared_ptr<file_sink> fs = shared_ptr<file_sink>(new file_sink(path)); 
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
  string expected = "Testing123";
  Assert.Equal(expected, actual);
  Assert.Equal(4, c);
}

FACT("File handle writes file by line")
{
  // Define input path
  string path = "out.txt";

  // Create a File Handle
  shared_ptr<FileHandle> f = createWriteFileHandle(path);

  string s0 = "Testing";
  string s1 = "1";
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
  string expected = "Testing123";
  Assert.Equal(expected, actual);
  Assert.Equal(4, c);
}

FACT("Endpoint read file by line. ScalarST Buffer")
{
  // Define input path
  string path = "in.txt";
  // Create FileHandle
  shared_ptr<FileHandle> f = createReadFileHandle(path);

  // Setup a K3 Endpoint
  auto buf = make_shared<ScalarEPBufferST>();
  Endpoint:SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings));

  tuple<int,string> res = readFile<Endpoint>(ep);  
  string actual = get<1>(res);
  int c = get<0>(res);
  
  // compare results
  string expected = "Testing123";
  Assert.Equal(expected, actual);
  Assert.Equal(4, c);
}

FACT("Endpoint read file by line. ContainerST Buffer")
{
  // Define input path
  string path = "in.txt";
  
  // Create FileHandle
  shared_ptr<FileHandle> f = createReadFileHandle(path);

  // Setup a K3 Endpoint
  auto buf = make_shared<ContainerEPBufferST>(BufferSpec(100,10));
  SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings));

  tuple<int,string> res = readFile<Endpoint>(ep);  
  string actual = get<1>(res);
  int c = get<0>(res);
  
  // compare results
  string expected = "Testing123";
  Assert.Equal(expected, actual);
  Assert.Equal(4, c);
}

FACT("Endpoint write file by line. ScalarEPBufferST")
{
  // Define input paths
  string path = "out.txt";

  // Create a File Handle
  shared_ptr<FileHandle> f = createWriteFileHandle(path);
  // Setup a K3 Endpoint
  auto buf = make_shared<ScalarEPBufferST>();
  Endpoint:SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings));

  string s0 = "Testing";
  string s1 = "1";
  string s2 = "2";
  string s3 = "3";

  ep->doWrite(s0);
  ep->doWrite(s1);
  ep->doWrite(s2);
  ep->doWrite(s3); 
  ep->flushBuffer();
  ep->close();
  shared_ptr<FileHandle> f2 = createReadFileHandle(path);

  tuple<int,string> res = readFile<FileHandle>(f2);  
  string actual = get<1>(res);
  int c = get<0>(res);
  
  // compare results
  string expected = "Testing123";
  Assert.Equal(expected, actual);
  Assert.Equal(4, c);
}

FACT("Endpoint write file by line. ContainerEPBufferST")
{
  // Define input paths
  string path = "out.txt";

  // Create a File Handle
  shared_ptr<FileHandle> f = createWriteFileHandle(path);
  // Setup a K3 Endpoint
  auto buf = make_shared<ContainerEPBufferST>(BufferSpec(100,10));
  Endpoint:SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  
  shared_ptr<Endpoint> ep = make_shared<Endpoint>(Endpoint(f, buf, bindings));

  string s0 = "Testing";
  string s1 = "1";
  string s2 = "2";
  string s3 = "3";

  ep->doWrite(s0);
  ep->doWrite(s1);
  ep->doWrite(s2);
  ep->doWrite(s3); 
  ep->flushBuffer();
  ep->close();
  shared_ptr<FileHandle> f2 = createReadFileHandle(path);

  tuple<int,string> res = readFile<FileHandle>(f2);  
  string actual = get<1>(res);
  int c = get<0>(res);
  
  // compare results
  string expected = "Testing123";
  Assert.Equal(expected, actual);
  Assert.Equal(4, c);
}

};
