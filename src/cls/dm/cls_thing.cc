#include <iostream>
#include "cls_thing.h"
#include "cls_dm.h"

void afunc() {
  std::cout << "blah" << std::endl ;

  // str
  Dataset ds( 0 ) ;
  std::cout << "type_code = " << ds.get_type_code() << std::endl ;
  ds.set_dataset( "blah" ) ;
  std::cout << "printed dataset:" << std::endl ;
  ds.print_dataset() ;
}
