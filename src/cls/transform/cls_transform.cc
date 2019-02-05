#include <iostream>
#include "cls_transform.h"

namespace Transform {

  int transform( librados::bufferlist* blist_in,
                 librados::bufferlist* blist_out,
                 int transform_op_id )
  {
    std::cout << "===================================================" << std::endl ;
    std::cout << "  EXECUTING transform()" << std::endl ;
    std::cout << "    transform_op_id : " << transform_op_id       << std::endl ;
    std::cout << std::endl ;

    switch( transform_op_id )
    {
      case 1 : transform_all( blist_in, blist_out ) ;      break ;
      case 2 : transform_reverse( blist_in, blist_out ) ;  break ;
      case 3 : transform_sort( blist_in, blist_out ) ;     break ;
      case 4 : transform_rowtocol( blist_in, blist_out ) ; break ;
      case 5 : transform_coltorow( blist_in, blist_out ) ; break ;
      default : transform_noop( blist_in, blist_out ) ;    break ;
    }

    return 0 ;
  }


  int transform_all( librados::bufferlist* blist_in,
                     librados::bufferlist* blist_out )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING transform_all()" << std::endl ;
    std::cout << std::endl ;

    // this is easier with a bufferptr
    ceph::bufferptr bptr_in ;
    librados::bufferlist::iterator i( &*blist_in ) ;
    i.copy_deep( blist_in->length(), bptr_in ) ;

    // fill out list
    for( unsigned int i = 0; i < blist_in->length(); i++ )
    {
      blist_out->append( bptr_in[ i ] ) ;
    }

    return 0 ;
  }

  int transform_reverse( librados::bufferlist* blist_in,
                         librados::bufferlist* blist_out )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING transform_reverse()" << std::endl ;
    std::cout << std::endl ;

    // this is easier with a bufferptr
    ceph::bufferptr bptr_in ;
    librados::bufferlist::iterator i( &*blist_in ) ;
    i.copy_deep( blist_in->length(), bptr_in ) ;

    // fill out list
    for( int i = blist_in->length(); i --> 0 ; )
    {
      blist_out->append( bptr_in[ i ] ) ;
    }

    return 0 ;
  }

  int transform_sort( librados::bufferlist* blist_in,
                      librados::bufferlist* blist_out )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING transform_sort()" << std::endl ;
    std::cout << std::endl ;

    auto data = (std::string)blist_in->c_str() ;
    std::sort( data.begin(), data.end() ) ;

    // fill out list
    char data_sorted[ blist_in->length() + 1 ] ;
    std::copy( data.begin(), data.end(), data_sorted ) ;
    blist_out->append( data_sorted, blist_in->length() ) ;

    return 0 ;
  }

  int transform_rowtocol( librados::bufferlist* blist_in,
                          librados::bufferlist* blist_out )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING transform_rowtocol()" << std::endl ;
    std::cout << std::endl ;
    return 0 ;
  }

  int transform_coltorow( librados::bufferlist* blist_in,
                          librados::bufferlist* blist_out )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING transform_coltorow()" << std::endl ;
    std::cout << std::endl ;
    return 0 ;
  }

  int transform_noop( librados::bufferlist* blist_in,
                      librados::bufferlist* blist_out )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING transform_noop()" << std::endl ;
    std::cout << std::endl ;
    return 0 ;
  }

}
