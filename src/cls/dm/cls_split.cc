#include <iostream>
#include "cls_split.h"
#include "cls_dm.h"

// ==================== //
//      CEPH SPLIT      //
// ==================== //
int ceph_split( librados::bufferlist* blist_in,
           librados::bufferlist* blist_out0, 
           librados::bufferlist* blist_out1, 
           int type_code,
           int split_op_id,
           const char* split_pattern )
{
  std::cout << "===================================================" << std::endl ;
  std::cout << "  EXECUTING split()" << std::endl ;
  std::cout << "    split_op_id : " << split_op_id       << std::endl ;
  std::cout << std::endl ;

  switch( split_op_id )
  {
    case 1  : ceph_split_12( blist_in, blist_out0, blist_out1, type_code ) ;                     break ;
//    case 2  : ceph_split_pattern( blist_in, blist_out0, blist_out1, split_pattern ) ; break ;
//    default : ceph_split_noop( blist_in, blist_out0, blist_out1 ) ;                   break ;
  }

  return 0 ;
}

// ======================= //
//      CEPH SPLIT 12      //
// ======================= //
int ceph_split_12( librados::bufferlist* blist_in,
                   librados::bufferlist* blist_out0, 
                   librados::bufferlist* blist_out1,
                   int type_code )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_split_12()" << std::endl ;
  std::cout << std::endl ;

//  std::string decoded_blist ;
//  librados::bufferlist::iterator biter( &*blist_in ) ;
//  ::decode( decoded_blist, biter ) ;
//  Dataset ds_str( 0 ) ;
//  ds_str.set_dataset( decoded_blist ) ;
//
//  //int midpoint = decoded_table[0].data.size() / 2 ;
//  int midpoint = ds_str.get_midpoint() ;

//  dataset_split left ;
//  dataset_split right ;
//
//  for( int i = 0; i < midpoint; i++ ) {
//    left.data.push_back( decoded_table[0].data[i] ) ;
//  }
//  for( unsigned int i = midpoint; i < decoded_table[0].data.size(); i++ ) {
//    right.data.push_back( decoded_table[0].data[i] ) ;
//  }
//
//  //std::cout << left.toString() << std::endl ;
//  //std::cout << right.toString() << std::endl ;
//
//  std::vector< dataset_split > left_table ;
//  left_table.push_back( left ) ;
//  ::encode( left_table, *blist_out0 ) ;
//
//  std::vector< dataset_split > right_table ;
//  right_table.push_back( right ) ;
//  ::encode( right_table, *blist_out1 ) ;

  return 0 ;
}

//// ============================ //
////      CEPH SPLIT PATTERN      //
//// ============================ //
//int ceph_split_pattern( librados::bufferlist* blist_in, 
//                        librados::bufferlist* blist_out0, 
//                        librados::bufferlist* blist_out1, 
//                        const char* split_pattern )
//{
//  std::cout << "............................" << std::endl ;
//  std::cout << "  EXECUTING split_pattern()" << std::endl ;
//  std::cout << "    split_pattern   : " << split_pattern << std::endl ;
//  std::cout << std::endl ;
//
//  std::vector< dataset_split > decoded_table ;
//  librados::bufferlist::iterator biter( &*blist_in ) ;
//  ::decode( decoded_table, biter ) ;
//
//  int pos = -1 ;
//  for( unsigned int i = 0; i < decoded_table[0].data.size(); i++ ) {
//    auto apair = decoded_table[0].data[i] ;
//    if( apair.first == split_pattern ) {
//      pos = i ;
//      break ;
//    }
//  } 
//
//  dataset_split left ;
//  dataset_split right ;
//
//  for( int i = 0; i < pos; i++ ) {
//    left.data.push_back( decoded_table[0].data[i] ) ;
//  }
//  for( unsigned int i = pos; i < decoded_table[0].data.size(); i++ ) {
//    right.data.push_back( decoded_table[0].data[i] ) ;
//  }
//
//  //std::cout << left.toString() << std::endl ;
//  //std::cout << right.toString() << std::endl ;
//
//  std::vector< dataset_split > left_table ;
//  left_table.push_back( left ) ;
//  ::encode( left_table, *blist_out0 ) ;
//
//  std::vector< dataset_split > right_table ;
//  right_table.push_back( right ) ;
//  ::encode( right_table, *blist_out1 ) ;
//
//  return 0 ;
//}

// ========================= //
//      CEPH SPLIT NOOP      //
// ========================= //
int ceph_split_noop( librados::bufferlist* blist_in, 
                librados::bufferlist* blist_out0,
                librados::bufferlist* blist_out1 )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING split_noop()" << std::endl ;
  std::cout << std::endl ;
  return 0 ;
}
