/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include "cls_transform_utils.h"
#include "include/rados/librados.hpp"

#include "rows_generated.h"

void execute_query( spj_query_op q_op, std::string layout ) {
  std::cout << "in execute_query..." << std::endl ;
  std::cout << "q_op.oid         : " << q_op.oid         << std::endl ;
  std::cout << "q_op.pool        : " << q_op.pool        << std::endl ;
  std::cout << "q_op.select_atts : " << q_op.select_atts << std::endl ;
  std::cout << "q_op.from_rels   : " << q_op.from_rels   << std::endl ;
  std::cout << "q_op.where_preds : " << q_op.where_preds << std::endl ;

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( q_op.pool.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( q_op.oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;
  std::cout << "num_bytes_read : " << num_bytes_read << std::endl ; 

  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // ================================================================================ //
  // display data

  // TODO: only display fields from select
  // TODO: add predicate filters

  if( layout == "ROW" ) {
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator

    const char* fb = bl.c_str() ;

    auto root = Tables::GetRows( fb ) ;
    auto table_name_read = root->table_name() ;
    auto schema_read     = root->schema() ;
    auto nrows_read      = root->nrows() ;
    auto ncols_read      = root->ncols() ;
    auto rids_read       = root->RIDs() ;
    auto att0_read       = root->att0() ;
    auto att1_read       = root->att1() ;

    std::cout << "table_name_read->str() : " << table_name_read->str() << std::endl ;
    std::cout << "schema_read->Length() : " << schema_read->Length() << std::endl ;
    std::cout << "schema_read->Get( 0 )->str() : " << schema_read->Get( 0 )->str() << std::endl ;
    std::cout << "schema_read->Get( 1 )->str() : " << schema_read->Get( 1 )->str() << std::endl ;
    std::cout << "nrows_read  : " << nrows_read << std::endl ;
    std::cout << "ncols_read  : " << ncols_read << std::endl ;
    std::cout << "RIDs : " << rids_read << std::endl ;
    std::cout << rids_read->Length() << std::endl ;
    std::cout << rids_read->Get(0) << std::endl ;
    std::cout << rids_read->Get(1) << std::endl ;
    std::cout << rids_read->Get(2) << std::endl ;

    std::cout << "att0 : " << att0_read << std::endl ;
    std::cout << att0_read->Length() << std::endl ;
    std::cout << att0_read->Get(0) << std::endl ;
    std::cout << att0_read->Get(1) << std::endl ;
    std::cout << att0_read->Get(2) << std::endl ;

    std::cout << "att1 : " << att1_read << std::endl ;
    std::cout << att1_read->Length() << std::endl ;
    std::cout << att1_read->Get(0) << std::endl ;
    std::cout << att1_read->Get(1) << std::endl ;
    std::cout << att1_read->Get(2) << std::endl ;
  }

  else if( layout == "COL" ) {
    ceph::bufferlist bl0 ;
    ::decode( bl0, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* ints_fb = bl0.c_str() ;
    auto ints_root      = Tables::GetCols_int( ints_fb ) ;

    auto int_data_read = ints_root->data() ;
    std::cout << "int_data_read->Length() : " << int_data_read->Length() << std::endl ;
    std::cout << int_data_read->Get( 0 ) << std::endl ;
    std::cout << int_data_read->Get( 1 ) << std::endl ;
    std::cout << int_data_read->Get( 2 ) << std::endl ;

    ceph::bufferlist bl1 ;
    ::decode( bl1, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* floats_fb = bl1.c_str() ;
    auto floats_root      = Tables::GetCols_float( floats_fb ) ;
  
    auto float_att0_read = floats_root->data() ;
    std::cout << "float_att0_read->Length() : " << float_att0_read->Length() << std::endl ;
    std::cout << float_att0_read->Get( 0 ) << std::endl ;
    std::cout << float_att0_read->Get( 1 ) << std::endl ;
    std::cout << float_att0_read->Get( 2 ) << std::endl ;
  }

  else {
    std::cout << "lol wut?" << std::endl ;
  }

  ioctx.close() ;
}

void execute_transform( transform_op t_op ) {
  std::cout << "in execute_transform..." << std::endl ;
  std::cout << "t_op.oid            = " << t_op.oid            << std::endl ;
  std::cout << "t_op.pool           = " << t_op.pool           << std::endl ;
  std::cout << "t_op.table_name     = " << t_op.table_name     << std::endl ;
  std::cout << "t_op.transform_type = " << t_op.transform_type << std::endl ;
  std::cout << "t_op.layout         = " << t_op.layout         << std::endl ;

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( t_op.pool.c_str(), ioctx ) ;
  checkret( ret, 0 ) ;

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( t_op.oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;
  std::cout << "num_bytes_read : " << num_bytes_read << std::endl ;

  auto transposed_bl_seq = transpose( wrapped_bl_seq ) ;

  // write bl_seq to ceph object
  auto obj_oid = t_op.oid + "_transposed" ;
  const char *obj_name  = obj_oid.c_str() ;
  bufferlist::iterator p = transposed_bl_seq.begin();
  size_t i = p.get_remaining() ;
  ret = ioctx.write( obj_name, transposed_bl_seq, i, 0 ) ;
  checkret( ret, 0 ) ;

  ioctx.close() ;
}

librados::bufferlist transpose( librados::bufferlist wrapped_bl_seq ) {
  std::cout << "in transpose..." << std::endl ;

  librados::bufferlist transposed_bl_seq ;
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  ceph::bufferlist bl ;
  ::decode( bl, it_wrapped ) ;
  const char* fb = bl.c_str() ;
  auto root = Tables::GetRows( fb ) ;

  //auto table_name  = root->table_name() ;
  auto rids_read   = root->RIDs() ;
  auto ints_read   = root->att0() ;
  auto floats_read = root->att1() ;

  std::vector< uint64_t > rids_vect ; 
  for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
    rids_vect.push_back( rids_read->Get( i ) ) ;
  }
  std::vector< uint64_t > ints_vect ; 
  for( unsigned int i = 0; i < ints_read->Length(); i++ ) {
    ints_vect.push_back( ints_read->Get( i ) ) ;
  }
  std::vector< float > floats_vect ; 
  for( unsigned int i = 0; i < floats_read->Length(); i++ ) {
    floats_vect.push_back( floats_read->Get( i ) ) ;
  }

  // ----------------------------------------------- //
  // build int column

  flatbuffers::FlatBufferBuilder ints_builder(1024) ;

  //place these before record_builder declare
  auto i_rids_vect_fb = ints_builder.CreateVector( rids_vect ) ;
  auto ints_vect_fb   = ints_builder.CreateVector( ints_vect ) ;
  auto int_layout     = ints_builder.CreateString( "COL" ) ;

  Tables::Cols_intBuilder cols_int_builder( ints_builder ) ;
  cols_int_builder.add_layout( int_layout ) ;
  cols_int_builder.add_RIDs( i_rids_vect_fb ) ;
  cols_int_builder.add_data( ints_vect_fb ) ;

  auto cols_int = cols_int_builder.Finish() ;
  ints_builder.Finish( cols_int ) ;

  // save record in bufferlist bl
  const char* ints_fb = reinterpret_cast<char*>( ints_builder.GetBufferPointer() ) ;

  int int_bfsz = ints_builder.GetSize() ;
  librados::bufferlist bl_ints ;
  bl_ints.append( ints_fb, int_bfsz ) ;

  // append to transposed_bl_seq bufferlist
  ::encode( bl_ints, transposed_bl_seq ) ;

  // ----------------------------------------------- //
  // build float column

  flatbuffers::FlatBufferBuilder floats_builder(1024) ;

  //place these before record_builder declare
  auto f_rids_vect_fb = floats_builder.CreateVector( rids_vect ) ;
  auto floats_vect_fb = floats_builder.CreateVector( floats_vect ) ;
  auto float_layout   = floats_builder.CreateString( "COL" ) ;

  Tables::Cols_floatBuilder cols_float_builder( floats_builder ) ;
  cols_float_builder.add_layout( float_layout ) ;
  cols_float_builder.add_RIDs( f_rids_vect_fb ) ;
  cols_float_builder.add_data( floats_vect_fb ) ;

  auto cols_float = cols_float_builder.Finish() ;
  floats_builder.Finish( cols_float ) ;

  // save record in bufferlist bl
  const char* floats_fb = reinterpret_cast<char*>( floats_builder.GetBufferPointer() ) ;

  int float_bfsz = floats_builder.GetSize() ;
  librados::bufferlist bl_floats ;
  bl_floats.append( floats_fb, float_bfsz ) ;

  // append to transposed_bl_seq bufferlist
  ::encode( bl_floats, transposed_bl_seq ) ;

  return transposed_bl_seq ;
}
