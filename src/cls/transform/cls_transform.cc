#include <iostream>
#include "cls_transform.h"

namespace Transform {

  int transform()
  {
    std::cout << "executing transform()" << std::endl ;

    transform_noop() ;
    transform_all() ;
    transform_reverse() ;
    transform_sort() ;
    transform_rowtocol() ;
    transform_coltorow() ;

    return 0 ;
  }

  int transform_noop()
  {
    std::cout << "executing transform_noop()" << std::endl ;
    return 0 ;
  }

  int transform_all()
  {
    std::cout << "executing transform_all()" << std::endl ;
    return 0 ;
  }

  int transform_reverse()
  {
    std::cout << "executing transform_reverse()" << std::endl ;
    return 0 ;
  }

  int transform_sort()
  {
    std::cout << "executing transform_sort()" << std::endl ;
    return 0 ;
  }

  int transform_rowtocol()
  {
    std::cout << "executing transform_rowtocol()" << std::endl ;
    return 0 ;
  }

  int transform_coltorow()
  {
    std::cout << "executing transform_coltorow()" << std::endl ;
    return 0 ;
  }
}
