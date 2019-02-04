#include <iostream>
#include "cls_split.h"

namespace Split {

  // =============== //
  //      SPLIT      //
  // =============== //
  int split( librados::bufferlist* blist_in,
             librados::bufferlist* blist_out0, 
             librados::bufferlist* blist_out1, 
             int split_op_id )
  {
    std::cout << "===================================================" << std::endl ;
    std::cout << "  EXECUTING split()" << std::endl ;
    std::cout << "    split_op_id : " << split_op_id       << std::endl ;
    std::cout << std::endl ;

    switch( split_op_id )
    {
      case 1  : split_12( blist_in, blist_out0, blist_out1 ) ;          break ;
      case 2  : split_pattern( blist_in, blist_out0, blist_out1, "" ) ; break ;
      default : split_noop( blist_in, blist_out0, blist_out1 ) ;        break ;
    }

    return 0 ;
  }

  // ================== //
  //      SPLIT 12      //
  // ================== //
  int split_12( librados::bufferlist* blist_in,
                librados::bufferlist* blist_out0, 
                librados::bufferlist* blist_out1 )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING split_12()" << std::endl ;
    std::cout << std::endl ;

    // this is easier with a bufferptr
    ceph::bufferptr bptr_in ;
    librados::bufferlist::iterator i( &*blist_in ) ;
    i.copy_deep( blist_in->length(), bptr_in ) ;

    // fill first list
    for( unsigned int i = 0; i < blist_in->length() / 2; i++ )
    {
      blist_out0->append( bptr_in[ i ] ) ;
    }

    // fill second list
    for( unsigned int i = blist_in->length() / 2; i < blist_in->length(); i++ )
    {
      blist_out1->append( bptr_in[ i ] ) ;
    }

    std::cout << "METHOD check:"      << std::endl ;
    std::cout << blist_in->c_str()    << std::endl ;
    std::cout << blist_in->length()   << std::endl ;
    std::cout << blist_out0->c_str()  << std::endl ;
    std::cout << blist_out0->length() << std::endl ;
    std::cout << blist_out1->c_str()  << std::endl ;
    std::cout << blist_out1->length() << std::endl ;

    return 0 ;
  }

  // ======================= //
  //      SPLIT PATTERN      //
  // ======================= //
  int split_pattern( librados::bufferlist* blist_in, 
                     librados::bufferlist* blist_out0, 
                     librados::bufferlist* blist_out1, 
                     const char* pattern )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING split_pattern()" << std::endl ;
    std::cout << "    pattern   : " << pattern << std::endl ;
    std::cout << std::endl ;

    auto data = (std::string)blist_in->c_str() ;
    auto pos  = data.find( "s" ) ;
    std::cout << data << std::endl ;
    std::cout << pos << std::endl ;

    // this is easier with a bufferptr
    ceph::bufferptr bptr_in ;
    librados::bufferlist::iterator i( &*blist_in ) ;
    i.copy_deep( blist_in->length(), bptr_in ) ;

    // fill first list
    for( unsigned int i = 0; i < pos; i++ )
    {
      blist_out0->append( bptr_in[ i ] ) ;
    }

    // fill second list
    for( unsigned int i = pos; i < blist_in->length(); i++ )
    {
      blist_out1->append( bptr_in[ i ] ) ;
    }

    std::cout << "METHOD check:"      << std::endl ;
    std::cout << blist_in->c_str()    << std::endl ;
    std::cout << blist_in->length()   << std::endl ;
    std::cout << blist_out0->c_str()  << std::endl ;
    std::cout << blist_out0->length() << std::endl ;
    std::cout << blist_out1->c_str()  << std::endl ;
    std::cout << blist_out1->length() << std::endl ;

    return 0 ;
  }

  // ==================== //
  //      SPLIT NOOP      //
  // ==================== //
  int split_noop( librados::bufferlist* blist_in, 
                  librados::bufferlist* blist_out0,
                  librados::bufferlist* blist_out1 )
  {
    std::cout << "............................" << std::endl ;
    std::cout << "  EXECUTING split_noop()" << std::endl ;
    std::cout << std::endl ;

    // this is easier with a bufferptr
    ceph::bufferptr bptr_in ;
    librados::bufferlist::iterator i( &*blist_in ) ;
    i.copy_deep( blist_in->length(), bptr_in ) ;

    // fill first list
    for( unsigned int i = 0; i < blist_in->length(); i++ )
    {
      blist_out0->append( bptr_in[ i ] ) ;
    }

    // fill second list
    for( unsigned int i = 0; i < blist_in->length(); i++ )
    {
      blist_out1->append( bptr_in[ i ] ) ;
    }

    std::cout << "METHOD check:"      << std::endl ;
    std::cout << blist_in->c_str()    << std::endl ;
    std::cout << blist_in->length()   << std::endl ;
    std::cout << blist_out0->c_str()  << std::endl ;
    std::cout << blist_out0->length() << std::endl ;
    std::cout << blist_out1->c_str()  << std::endl ;
    std::cout << blist_out1->length() << std::endl ;

    return 0 ;
  }
}
