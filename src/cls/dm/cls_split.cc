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
           std::string split_pattern )
{
  std::cout << "===================================================" << std::endl ;
  std::cout << "  EXECUTING split()" << std::endl ;
  std::cout << "    split_op_id : " << split_op_id       << std::endl ;
  std::cout << std::endl ;

  switch( split_op_id )
  {
    case 0 : 
      ceph_split_id( blist_in, blist_out0, blist_out1, type_code ) ;
      break ;
    case 1 : 
      ceph_split_12( blist_in, blist_out0, blist_out1, type_code ) ;
      break ;
    case 2 : 
      ceph_split_pattern( blist_in, blist_out0, blist_out1, split_pattern ) ;
      break ;
    case 3 : 
      ceph_split_hash2( blist_in, blist_out0, blist_out1 ) ;
      break ;
    default :
      ceph_split_noop( blist_in, blist_out0, blist_out1 ) ;
      break ;
  }

  return 0 ;
}

int ceph_split( librados::bufferlist* blist_in,
           librados::bufferlist* blist_out0, 
           librados::bufferlist* blist_out1, 
           int type_code,
           int split_op_id,
           uint64_t split_pattern )
{
  std::cout << "===================================================" << std::endl ;
  std::cout << "  EXECUTING split()" << std::endl ;
  std::cout << "    split_op_id   : " << split_op_id   << std::endl ;
  std::cout << "    split_pattern : " << split_pattern << std::endl ;
  std::cout << std::endl ;

  ceph_split_pattern( blist_in, blist_out0, blist_out1, split_pattern ) ;

  return 0 ;
}

// ======================= //
//      CEPH SPLIT ID      //
// ======================= //
int ceph_split_id( librados::bufferlist* blist_in,
                   librados::bufferlist* blist_out0, 
                   librados::bufferlist* blist_out1,
                   int type_code )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_split_id()" << std::endl ;
  std::cout << "    type_code = " << type_code << std::endl ; 
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;
  Dataset left  = decoded_dataset ;
  Dataset right = decoded_dataset ;

  std::cout << "left:" << std::endl ; 
  std::cout << left.toString() << std::endl ;
  std::cout << "right:" << std::endl ; 
  std::cout << right.toString() << std::endl ;

  ::encode( left, *blist_out0 ) ;
  ::encode( right, *blist_out1 ) ;

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

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;

  int midpoint = get_midpoint( decoded_dataset ) ;
  std::cout << "midpoint:" << std::endl ;
  std::cout << midpoint << std::endl ;

  Dataset left  = get_range( decoded_dataset, 0, midpoint ) ;
  Dataset right = get_range( decoded_dataset, midpoint, decoded_dataset.getLength() ) ;

  std::cout << "left:" << std::endl ;
  std::cout << left.toString() << std::endl ;
  std::cout << "right:" << std::endl ;
  std::cout << right.toString() << std::endl ;

  ::encode( left, *blist_out0 ) ;
  ::encode( right, *blist_out1 ) ;

  return 0 ;
}

// ============================ //
//      CEPH SPLIT PATTERN      //
// ============================ //
int ceph_split_pattern( librados::bufferlist* blist_in, 
                        librados::bufferlist* blist_out0, 
                        librados::bufferlist* blist_out1, 
                        std::string split_pattern )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING split_pattern()" << std::endl ;
  std::cout << "    split_pattern   : " << split_pattern << std::endl ;
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;

  int pattern_index = get_index( decoded_dataset, split_pattern ) ;
  std::cout << "pattern_index:" << std::endl ;
  std::cout << pattern_index << std::endl ;

  Dataset left  = get_range( decoded_dataset, 0, pattern_index ) ;
  Dataset right = get_range( decoded_dataset, 
                             pattern_index, 
                             decoded_dataset.getLength() ) ;

  std::cout << "left:" << std::endl ;
  std::cout << left.toString() << std::endl ;
  std::cout << "right:" << std::endl ;
  std::cout << right.toString() << std::endl ;

  ::encode( left, *blist_out0 ) ;
  ::encode( right, *blist_out1 ) ;

  return 0 ;
}

int ceph_split_pattern( librados::bufferlist* blist_in, 
                        librados::bufferlist* blist_out0, 
                        librados::bufferlist* blist_out1, 
                        uint64_t split_pattern )
{
  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING split_pattern()" << std::endl ;
  std::cout << "    split_pattern   : " << split_pattern << std::endl ;
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;

  int pattern_index = get_index( decoded_dataset, split_pattern ) ;
  std::cout << "pattern_index:" << std::endl ;
  std::cout << pattern_index << std::endl ;

  Dataset left  = get_range( decoded_dataset, 0, pattern_index ) ;
  Dataset right = get_range( decoded_dataset, 
                             pattern_index, 
                             decoded_dataset.getLength() ) ;

  std::cout << "left:" << std::endl ;
  std::cout << left.toString() << std::endl ;
  std::cout << "right:" << std::endl ;
  std::cout << right.toString() << std::endl ;

  ::encode( left, *blist_out0 ) ;
  ::encode( right, *blist_out1 ) ;

  return 0 ;
}

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

// ========================== //
//      CEPH SPLIT HASH2      //
// ========================== //
int ceph_split_hash2( librados::bufferlist* blist_in,
                      librados::bufferlist* blist_out0,
                      librados::bufferlist* blist_out1 ) {

  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_split_hash2()" << std::endl ;
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;

  int type_code = decoded_dataset.type_code ;
  Dataset left( type_code ) ;
  Dataset right( type_code ) ;

  for( int i = 0; i < decoded_dataset.getLength(); i++ ) {
    if( i % 2 == 0 ) {
      switch( type_code ) {
        case 2 :
          left.append( decoded_dataset.get_uint64( i ) ) ;
          break ;
        case 3 :
          left.append( decoded_dataset.get_vvop( i ) ) ;
          break ;
        default :
          left.append( decoded_dataset.get( i ) ) ;
          break ;
      }
    }
    else {
      switch( type_code ) {
        case 2 :
          right.append( decoded_dataset.get_uint64( i ) ) ;
          break ;
        case 3 :
          right.append( decoded_dataset.get_vvop( i ) ) ;
          break ;
        default :
          right.append( decoded_dataset.get( i ) ) ;
          break ;
      }
    }
  }

  std::cout << "left:" << std::endl ; 
  std::cout << left.toString() << std::endl ;
  std::cout << "right:" << std::endl ; 
  std::cout << right.toString() << std::endl ;

  ::encode( left, *blist_out0 ) ;
  ::encode( right, *blist_out1 ) ;

  return 0 ;
}
