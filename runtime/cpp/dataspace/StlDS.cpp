 #include <iostream>
 #include <set>
 #include <string>
 #include <functional>
 #include <Engine.hpp>

    using K3::Engine;
    using std::shared_ptr;
 template <typename Elem, template<typename, typename=std::allocator<Elem>> class Container>
 class StlDS
  {
    typedef typename Container<Elem>::const_iterator iterator_type;
    protected:
      Container<Elem> container;

      public:
      StlDS(Engine * eng) : container()
      {}

      template<typename Iterator>
      StlDS(Engine * eng, Iterator start, Iterator finish)
          : container(start,finish)
      { }

      StlDS(const StlDS& other)
          : container(other.container)
      { }

    public:
      shared_ptr<Elem> peek() const
      {
        shared_ptr<Elem> res;
        iterator_type it = container.begin();
        if (it != container.end()) {
          res = std::make_shared<Elem>(*it);
        }
        return res;
      }

      void insert_basic(const Elem& v)
      {
        container.insert(container.end(),v);
      }

      void delete_first(const Elem& v)
      {
        iterator_type it = container.begin();
        container.erase(it);
      }

      void update_first(const Elem& v, const Elem& v2)
      {
        iterator_type it;
        for(it = container.begin(); it != container.end(); it++) {
          Elem curr = *it;
          if (curr == v) {
            container.emplace(it,v2);
            container.erase(it);
            return;
          }
        }
      }

      template<typename Acc>
      Acc fold(std::function<Acc(Acc, Elem)> f, Acc init_acc)
      {
        Acc acc = init_acc;
        for (Elem e : container) {
          acc = f(acc, e);
        }  
        return acc;
      }

      template<typename NewElem>
      StlDS<NewElem, Container> map(std::function<NewElem(Elem)> f)
      {
        StlDS<NewElem, Container> result = StlDS<NewElem, Container>(nullptr);
        for (Elem e : container) {
          NewElem new_e = f(e);
          result.insert_basic(new_e);
        }

        return result;
      }

      void iterate(std::function<void(Elem)> f)
      {
        for (Elem e : container) {
          f(e);
        }
      }

      // StlDS filter(std::function<bool(Elem)> predicate)
      // {
      //   StlDS<Elem, Container> result();
      //   for (Elem e : container) {
      //     if (predicate(e)) {
      //       result.insert_basic(e);
      //     }
      //   }
      //   return result;
      // }

      // tuple< FileDS, FileDS > split()
      // {
      //   tuple<Identifier, Identifier> halves = splitFile(engine, file_id);
      //   return make_tuple(
      //           FileDS(engine, get<0>(halves)),
      //           FileDS(engine, get<1>(halves))
      //           );
      // }
      // FileDS combine(FileDS other)
      // {
      //   return FileDS(engine,
      //           combineFile(engine, file_id, other.file_id)
      //           );
      // }
    
};
using namespace std;
using std::string;

int sum_ds(int acc, int elem) {
  return acc+elem;
}

string check_even(int elem) {
  if (elem % 2 == 0) {
    return "even!";
  } else {
    return "odd!";
  }
}

template<typename E>
void print_elem(E elem) {
  cout << elem << endl;
}


int main() {
  K3::Engine * e;
  StlDS<int, std::list> ds = StlDS<int, std::list>(e);
  //Empty peek
  auto x = ds.peek();
  if (x) {
    cout << "Failed empty peek" << endl;
  }
  // Insert and peek
  ds.insert_basic(5);
  ds.insert_basic(6);
  x = ds.peek();
  if (!x) {
    cout << "Failed a peek" << endl;
  } else {
    std::cout << *x << std::endl;
  }
  // Delete and peek
  ds.delete_first(5);
  x = ds.peek();
  cout << "should be 6:" << *x << endl;
  // Update and peek
  ds.update_first(6,7);
    x = ds.peek();
  cout << "should be 7:" << *x << endl;
  ds.delete_first(7);
  // Now empty:
  x = ds.peek();
  if (x) {
    cout << "failed: should be empty" << endl;
  } else {
    cout << "success: its empty" << endl;
  }
  // Fold
  ds.insert_basic(1);
  ds.insert_basic(2);
  ds.insert_basic(3);
  std::function<int(int,int)> folder = sum_ds;
  int s = ds.fold(folder, 0);
  if (s != 6) {
    cout << "failed fold!" << endl;
  } else {
    cout << "fold success" << endl;
  }
  // Map
  std::function<string(int)> mapper = check_even;
  StlDS<string, std::list> ds2 = ds.map(mapper);
  // Iter
  std::function<void(string)> printer = print_elem<string>;
  ds2.iterate(printer);


  
  return 0;
}