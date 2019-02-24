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

  std::vector< dataset > decoded_table ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_table, biter ) ;

  dataset ds_reversed ;
  for( int i = decoded_table[0].data.size() - 1 ; i >=0 ; i-- ) {
    ds_reversed.data.push_back( decoded_table[0].data[i] ) ;
  }

  std::vector< dataset > reversed_table ;
  reversed_table.push_back( ds_reversed ) ;
  ::encode( reversed_table, *blist_out ) ;

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

  std::vector< dataset > decoded_table ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_table, biter ) ;

  std::sort( decoded_table[0].data.begin(), decoded_table[0].data.end() ) ;

  std::vector< dataset > sorted_table ;
  sorted_table.push_back( decoded_table[0] ) ;
  ::encode( sorted_table, *blist_out ) ;

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

  // one dataset per row == decoded_table.size()
  std::vector< dataset > transpose ;

  // peel out the ith element from every row and insert
  // into the corresponding ith col
  int num_transpose_rows = decoded_table[0].getLength() ;
  for( int i = 0; i < num_transpose_rows; i++ ) {
    dataset ds ;
    for( unsigned int j = 0; j < decoded_table.size(); j++ ) {
      ds.data.push_back( decoded_table[j].data[i] ) ;
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
