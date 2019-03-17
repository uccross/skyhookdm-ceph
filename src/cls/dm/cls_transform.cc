#include <iostream>
#include "cls_transform.h"
#include "cls_dm.h"

int ceph_transform( librados::bufferlist* blist_in,
                    librados::bufferlist* blist_out,
                    int ceph_transform_op_id ) {

  std::cout << "===================================================" << std::endl ;
  std::cout << "  EXECUTING ceph_transform()" << std::endl ;
  std::cout << "    ceph_transform_op_id : " << ceph_transform_op_id << std::endl ;
  std::cout << std::endl ;

  switch( ceph_transform_op_id )
  {
    case 0 : ceph_transform_id( blist_in, blist_out ) ;      break ;
    case 1 : ceph_transform_reverse( blist_in, blist_out ) ;  break ;
    case 2 : ceph_transform_sort( blist_in, blist_out ) ;     break ;
    case 3 : ceph_transform_transpose( blist_in, blist_out ) ; break ;
    default : ceph_transform_noop( blist_in, blist_out ) ;    break ;
  }

  return 0 ;
}

int ceph_transform_id( librados::bufferlist* blist_in,
                       librados::bufferlist* blist_out ) {

  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_id()" << std::endl ;
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;
  Dataset ds_out = decoded_dataset ;

  std::cout << "ds_out:" << std::endl ;
  std::cout << ds_out.toString() << std::endl ;

  ::encode( ds_out, *blist_out ) ;

  return 0 ;
}

int ceph_transform_reverse( librados::bufferlist* blist_in,
                            librados::bufferlist* blist_out ) {

  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_reverse()" << std::endl ;
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  Dataset ds_out = get_reverse( decoded_dataset ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;
  std::cout << "ds_out:" << std::endl ;
  std::cout << ds_out.toString() << std::endl ;

  ::encode( ds_out, *blist_out ) ;

  return 0 ;
}

int ceph_transform_sort( librados::bufferlist* blist_in,
                         librados::bufferlist* blist_out ) {

  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_sort()" << std::endl ;
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  Dataset ds_out = get_sort( decoded_dataset ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;
  std::cout << "ds_out:" << std::endl ;
  std::cout << ds_out.toString() << std::endl ;

  ::encode( ds_out, *blist_out ) ;

  return 0 ;
}

int ceph_transform_transpose( librados::bufferlist* blist_in,
                              librados::bufferlist* blist_out ) {

  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_transpose()" << std::endl ;
  std::cout << std::endl ;

  Dataset decoded_dataset ;
  librados::bufferlist::iterator biter( &*blist_in ) ;
  ::decode( decoded_dataset, biter ) ;

  Dataset ds_out = get_transpose( decoded_dataset ) ;

  std::cout << "decoded_dataset:" << std::endl ;
  std::cout << decoded_dataset.toString() << std::endl ;
  std::cout << "decoded_dataset.getLength():" << std::endl ;
  std::cout << decoded_dataset.getLength() << std::endl ;
  std::cout << "ds_out:" << std::endl ;
  std::cout << ds_out.toString() << std::endl ;

  ::encode( ds_out, *blist_out ) ;

  return 0 ;
}

int ceph_transform_noop( librados::bufferlist* blist_in,
                         librados::bufferlist* blist_out ) {

  std::cout << "............................" << std::endl ;
  std::cout << "  EXECUTING ceph_transform_noop()" << std::endl ;
  std::cout << std::endl ;
  return 0 ;
}

