#include <iostream>
#include "cls_transform.h"

int ceph_transform( librados::bufferlist* blist_in,
               librados::bufferlist* blist_out,
               int ceph_transform_op_id )
{
  std::cout << "===================================================" << std::endl ;
  std::cout << "  EXECUTING ceph_transform()" << std::endl ;
  std::cout << "    ceph_transform_op_id : " << ceph_transform_op_id       << std::endl ;
  std::cout << std::endl ;

  switch( ceph_transform_op_id )
  {
    case 1 : ceph_transform_all( blist_in, blist_out ) ;      break ;
    case 2 : ceph_transform_reverse( blist_in, blist_out ) ;  break ;
    case 3 : ceph_transform_sort( blist_in, blist_out ) ;     break ;
    case 4 : ceph_transform_transpose( blist_in, blist_out ) ; break ;
    default : ceph_transform_noop( blist_in, blist_out ) ;    break ;
  }

  return 0 ;
}

// =========================== //
//      CEPH_TRANSFORM ALL     //
// =========================== //
int ceph_transform_all( librados::bufferlist* blist_in,
                        librados::bufferlist* blist_out )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_all()" << std::endl ;
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

// =============================== //
//      CEPH_TRANSFORM REVERSE     //
// =============================== //
int ceph_transform_reverse( librados::bufferlist* blist_in,
                            librados::bufferlist* blist_out )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_reverse()" << std::endl ;
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

// ============================ //
//      CEPH_TRANSFORM SORT     //
// ============================ //
int ceph_transform_sort( librados::bufferlist* blist_in,
                         librados::bufferlist* blist_out )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_sort()" << std::endl ;
  std::cout << std::endl ;

  auto data = (std::string)blist_in->c_str() ;
  std::sort( data.begin(), data.end() ) ;

  // fill out list
  char data_sorted[ blist_in->length() + 1 ] ;
  std::copy( data.begin(), data.end(), data_sorted ) ;
  blist_out->append( data_sorted, blist_in->length() ) ;

  return 0 ;
}

// ================================= //
//      CEPH_TRANSFORM TRANSPOSE     //
// ================================= //
int ceph_transform_transpose( librados::bufferlist* blist_in,
                             librados::bufferlist* blist_out )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_transpose()" << std::endl ;
  std::cout << std::endl ;

  std::vector< dataset > decoded_table ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_table, biter ) ;

  // initialize one dataset per row == decoded_table.size()
  std::vector< dataset > transpose ;

  // peel out the ith character from every row and insert
  // into the corresponding ith col
  for( int i = 0; i < decoded_table.size(); i++ ) {
    dataset ds ;
    ds.id   = i+1 ;
    ds.data = "" ;
    for( int j = 0; j < decoded_table[i].getLength(); j++ ) {
      ds.data += decoded_table[j].data[i] ;
    }
    transpose.push_back( ds ) ;
  }

  ::encode( transpose, *blist_out ) ;

  return 0 ;
}

// ============================ //
//      CEPH_TRANSFORM NOOP     //
// ============================ //
int ceph_transform_noop( librados::bufferlist* blist_in,
                    librados::bufferlist* blist_out )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_noop()" << std::endl ;
  std::cout << std::endl ;
  return 0 ;
}
