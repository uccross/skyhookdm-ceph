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

void execute_query( spj_query_op q_op ) {
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

  // grab the Root
  ceph::bufferlist bl ;
  ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
  const char* fb = bl.c_str() ;

  auto root      = Tables::GetRoot( fb ) ;
  auto data_type = root->relationData_type() ;

  std::cout << "data_type : " << data_type << std::endl ;

  // process one Root>Rows flatbuffer
  if( data_type == Tables::Relation_Rows ) {

    auto schema    = root->schema() ;
    std::cout << "schema : " << std::endl ;
    for( unsigned int i = 0; i < schema->Length(); i++ )
      std::cout << (unsigned)schema->Get(i) << std::endl ;
 
    auto rows = static_cast< const Tables::Rows* >( root->relationData() ) ;
    auto table_name_read = rows->table_name() ;
    auto schema_read     = rows->schema() ;
    auto nrows_read      = rows->nrows() ;
    auto ncols_read      = rows->ncols() ;
    auto rids_read       = rows->RIDs() ;
    auto rows_data_type  = rows->data_type() ;
    auto rows_data       = rows->data() ;

    std::cout << "table_name_read->str() : " << table_name_read->str() << std::endl ;
    std::cout << "schema_read->Length() : " << schema_read->Length() << std::endl ;
    std::cout << "nrows_read     : " << nrows_read     << std::endl ;
    std::cout << "ncols_read     : " << ncols_read     << std::endl ;

    // print schema to stdout
    std::cout << "RID\t" ;
    for( unsigned int i = 0; i < ncols_read; i++ ) {
      std::cout << schema_read->Get(i)->str() << "\t" ;
    }
    std::cout << std::endl ;

    //auto int_data = static_cast< const Tables::IntData* >( rows_data->Get(0) ) ;
    //std::cout << int_data->data()->Get(0) << std::endl ;

    // print data to stdout
    for( unsigned int i = 0; i < nrows_read; i++ ) {
      std::cout << rids_read->Get(i) << ":\t" ;
      for( unsigned int j = 0; j < ncols_read; j++ ) {

        // column of ints
        if( (unsigned)rows_data_type->Get(j) == Tables::Data_IntData ) {
          auto int_col_data = static_cast< const Tables::IntData* >( rows_data->Get(j) ) ;
          std::cout << int_col_data->data()->Get(i) << "\t" ;
        }
        // column of floats
        else if( (unsigned)rows_data_type->Get(j) == Tables::Data_FloatData ) {
          auto float_col_data = static_cast< const Tables::FloatData* >( rows_data->Get(j) ) ;
          std::cout << float_col_data->data()->Get(i) << "\t" ;
        }
        // column of strings
        else if( (unsigned)rows_data_type->Get(j) == Tables::Data_FloatData ) {
          auto float_col_data = static_cast< const Tables::FloatData* >( rows_data->Get(j) ) ;
          std::cout << float_col_data->data()->Get(i) << "\t" ;
        }
        else {
          std::cout << "cls_transform_utils.cc: wut???" << std::endl ; 
        }
      }
      std::cout << std::endl ;
    }
  } // Relation_Rows 

  // process one Root>Col flatbuffer
  else if( data_type == Tables::Relation_Col ) {
    std::cout << "else if data_type == Tables::Relation_Col" << std::endl ;

    auto col = static_cast< const Tables::Col* >( root->relationData() ) ;
    auto col_name_read  = col->col_name() ;
    auto col_index_read = col->col_index() ;
    auto nrows_read     = col->nrows() ;
    auto rids_read      = col->RIDs() ;
    auto col_data_type  = col->data_type() ;
    auto col_data       = col->data() ;

    std::cout << "col_name_read->str() : " << col_name_read->str() << std::endl ;
    std::cout << "col_index_read       : " << (unsigned)col_index_read << std::endl ;
    std::cout << "nrows_read           : " << (unsigned)nrows_read     << std::endl ;
    std::cout << "col_data_type        : " << col_data_type     << std::endl ;

    // print data to stdout
    for( unsigned int i = 0; i < nrows_read; i++ ) {
      std::cout << rids_read->Get(i) << ":\t" ;
      // column of ints
      if( (unsigned)col_data_type == Tables::Data_IntData ) {
        auto int_col_data = static_cast< const Tables::IntData* >( col_data ) ;
        std::cout << int_col_data->data()->Get(i) << "\t" ;
      }
      // column of floats
      else if( (unsigned)col_data_type == Tables::Data_FloatData ) {
        auto float_col_data = static_cast< const Tables::FloatData* >( col_data ) ;
        std::cout << float_col_data->data()->Get(i) << "\t" ;
      }
      // column of strings
      else if( (unsigned)col_data_type == Tables::Data_StringData ) {
        auto string_col_data = static_cast< const Tables::StringData* >( col_data ) ;
        std::cout << string_col_data->data()->Get(i) << "\t" ;
      }
      else {
        std::cout << "unrecognized data_type " << (unsigned)col_data_type << std::endl ; 
      }
      std::cout << std::endl ;
    }
  } // Relation_Col

  else {
    std::cout << "unrecognized data_type '" << data_type << "'" << std::endl ;
  }

  ioctx.close() ;
}

void execute_transform( transform_op t_op ) {
  std::cout << "in execute_transform..." << std::endl ;
  std::cout << "t_op.oid            = " << t_op.oid            << std::endl ;
  std::cout << "t_op.pool           = " << t_op.pool           << std::endl ;
  std::cout << "t_op.table_name     = " << t_op.table_name     << std::endl ;
  std::cout << "t_op.transform_type = " << t_op.transform_type << std::endl ;

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

  librados::bufferlist transposed_bl_seq ;
  switch( t_op.transform_type ) {
    case 0 :
      transposed_bl_seq = transpose( wrapped_bl_seq ) ;
      break ;
    default :
      std::cout << "lol wut? unrecognized t_op.transform_type '" << t_op.transform_type << "'" << std::endl ;
      break ;
  }

  // write bl_seq to ceph object
  auto obj_oid = t_op.oid + "_transposed" ;
  const char *obj_name  = obj_oid.c_str() ;
  bufferlist::iterator p = transposed_bl_seq.begin();
  size_t i = p.get_remaining() ;
  ret = ioctx.write( obj_name, transposed_bl_seq, i, 0 ) ;
  checkret( ret, 0 ) ;

// ------------------- \/\/\/ EXPERIMENTAL \/\/\/ ------------------- //
  // define transform operation for re-composition
//  transform_op to1 ;
//  to1.oid            = "blah2_transposed" ;
//  to1.pool           = "tpchflatbuf" ;
//  to1.table_name     = "atable" ;
//  to1.transform_type = 0 ; // 0 --> transpose
//
//  auto recomposed_bl_seq = transpose( transposed_bl_seq ) ;
//
//  // write bl_seq to ceph object
//  auto obj_oid1 = to1.oid + "_transposed" ;
//  const char *obj_name1  = obj_oid1.c_str() ;
//  bufferlist::iterator p1 = recomposed_bl_seq.begin();
//  size_t i1 = p1.get_remaining() ;
//  ret = ioctx.write( obj_name1, recomposed_bl_seq, i1, 0 ) ;
//  checkret( ret, 0 ) ;
// ------------------- /\/\/\ EXPERIMENTAL /\/\/\  ------------------- //

  ioctx.close() ;
}

librados::bufferlist transpose( librados::bufferlist wrapped_bl_seq ) {
  std::cout << "in transpose..." << std::endl ;

  librados::bufferlist transposed_bl_seq ;
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // grab the Root
  ceph::bufferlist bl ;
  ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
  const char* fb = bl.c_str() ;

  auto root      = Tables::GetRoot( fb ) ;
  auto schema    = root->schema() ;
  auto data_type = root->relationData_type() ;

  std::cout << "schema : " << std::endl ;
  for( unsigned int i = 0; i < schema->Length(); i++ )
    std::cout << (unsigned)schema->Get(i) << std::endl ;
  std::cout << "data_type : " << data_type << std::endl ;

  // process ROWs
  if( data_type == Tables::Relation_Rows ) {

    auto rows = static_cast< const Tables::Rows* >( root->relationData() ) ;
    auto table_name_read = rows->table_name() ;
    auto schema_read     = rows->schema() ;
    auto nrows_read      = rows->nrows() ;
    auto ncols_read      = rows->ncols() ;
    auto rids_read       = rows->RIDs() ;
    auto rows_data_type  = rows->data_type() ;
    auto rows_data       = rows->data() ;

    std::cout << "table_name_read->str() : " << table_name_read->str() << std::endl ;
    std::cout << "schema_read->Length() : " << schema_read->Length() << std::endl ;
    std::cout << "nrows_read     : " << nrows_read     << std::endl ;
    std::cout << "ncols_read     : " << ncols_read     << std::endl ;

    std::vector< uint8_t > schema_vect ;
    for( unsigned int i = 0; i < schema->Length(); i++ ) {
      schema_vect.push_back( schema->Get(i) ) ;
    }
    std::vector< uint64_t > RIDs_vect ;
    for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
      RIDs_vect.push_back( rids_read->Get(i) ) ;
    }

    // build out Root>Col
    flatbuffers::FlatBufferBuilder builder( 1024 ) ;
    auto schema_fb = builder.CreateVector( schema_vect ) ;
    auto RIDs      = builder.CreateVector( RIDs_vect ) ;

    for( unsigned int i = 0; i < ncols_read; i++ ) {

      // =============================== //
      // column of ints
      if( (unsigned)rows_data_type->Get(i) == Tables::Data_IntData ) {
        auto int_col_data_read = static_cast< const Tables::IntData* >( rows_data->Get(i) ) ;
        std::vector< uint64_t > int_col_data_vect ;
        for( unsigned int j = 0; j < int_col_data_read->data()->Length(); j++ ) {
          int_col_data_vect.push_back( int_col_data_read->data()->Get(j) ) ;
        }
        auto int_col_data_vect_fb = builder.CreateVector( int_col_data_vect ) ;

        auto int_col_data = Tables::CreateIntData( builder, int_col_data_vect_fb ) ;
        auto col_name = builder.CreateString( schema_read->Get(i)->str() ) ;

        auto col = CreateCol(
          builder,
          0,
          0,
          col_name,
          (uint8_t)0,
          nrows_read,
          RIDs,
          Tables::Data_IntData,
          int_col_data.Union() ) ;

        // save the Rows flatbuffer to the root flatbuffer
        Tables::RootBuilder root_builder( builder ) ;
        root_builder.add_schema( schema_fb ) ;
        root_builder.add_relationData_type( Tables::Relation_Col ) ;
        root_builder.add_relationData( col.Union() ) ;

        // save column to buffer and append to the transposed bufferlist
        auto res = root_builder.Finish() ;
        builder.Finish( res ) ;
        const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
        int bufsz      = builder.GetSize() ;
        librados::bufferlist bl ;
        bl.append( fb, bufsz ) ;
        ::encode( bl, transposed_bl_seq ) ;

        // ------------- sanity check ------------- //
        auto _fb = bl.c_str() ;
        auto _root          = Tables::GetRoot( _fb ) ;
        auto _schema        = _root->schema() ; //2
        auto _data_type     = _root->relationData_type() ; //2=>Col
        auto _data          = _root->relationData() ;
        auto _col           = static_cast< const Tables::Col* >( _data ) ;
        auto _col_name      = _col->col_name() ;
        auto _col_index     = _col->col_index() ;
        auto _nrows         = _col->nrows() ;
        auto _rids          = _col->RIDs() ;
        auto _col_data_type = _col->data_type() ;
        auto _col_int_data  = static_cast< const Tables::IntData* >( _col->data() ) ;
        auto _col_int_data_data = _col_int_data->data() ;

        std::cout << "TRANSPOSE : _schema->Length() : " << _schema->Length()         << std::endl ;
        for( unsigned int i = 0; i < _schema->Length(); i++ ) {
          std::cout << "TRANSPOSE : _schema->Get(" << i << ")   : " 
                    << (unsigned)_schema->Get(i) << std::endl ;
        }
        std::cout << "TRANSPOSE : _data_type        : " << _data_type                << std::endl ;
        std::cout << "TRANSPOSE : _col_name->str()  : " << _col_name->str()          << std::endl ;
        std::cout << "TRANSPOSE : _col_index        : " << (unsigned)_col_index      << std::endl ;
        std::cout << "TRANSPOSE : _nrows            : " << (unsigned)_nrows          << std::endl ;
        std::cout << "TRANSPOSE : _rids->Length()   : " << (unsigned)_rids->Length() << std::endl ;
        for( unsigned int i = 0; i < _rids->Length(); i++ ) {
          std::cout << "TRANSPOSE : _rids->Get(" << i << ")     : " 
                    << (unsigned)_rids->Get(i)   << std::endl ;
        }
        std::cout << "TRANSPOSE : _col_data_type    : " << _col_data_type << std::endl ;
        std::cout << "TRANSPOSE : _col_int_data_data->Length() : " 
                  << _col_int_data_data->Length() << std::endl ;
        for( unsigned int i = 0; i < _col_int_data_data->Length(); i++ ) {
          std::cout << "TRANSPOSE : _col_int_data_data->Get(" << i << ")     : " 
                    << (unsigned)_col_int_data_data->Get(i)   << std::endl ;
        }
        // ------------- sanity check ------------- //
      }

      // =============================== //
      // column of floats
      else if( (unsigned)rows_data_type->Get(i) == Tables::Data_FloatData ) {
        auto float_col_data_read = static_cast< const Tables::FloatData* >( rows_data->Get(i) ) ;
        std::vector< float > float_col_data_vect ;
        for( unsigned int j = 0; j < float_col_data_read->data()->Length(); j++ ) {
          float_col_data_vect.push_back( float_col_data_read->data()->Get(j) ) ;
        }
        auto float_col_data_vect_fb = builder.CreateVector( float_col_data_vect ) ;

        auto float_col_data = Tables::CreateFloatData( builder, float_col_data_vect_fb ) ;
        auto col_name = builder.CreateString( schema_read->Get(i)->str() ) ;

        auto col = CreateCol(
          builder,
          0,
          0,
          col_name,
          (uint8_t)0,
          nrows_read,
          RIDs,
          Tables::Data_FloatData,
          float_col_data.Union() ) ;

        // save the Rows flatbuffer to the root flatbuffer
        Tables::RootBuilder root_builder( builder ) ;
        root_builder.add_schema( schema_fb ) ;
        root_builder.add_relationData_type( Tables::Relation_Col ) ;
        root_builder.add_relationData( col.Union() ) ;

        // save column to buffer and append to the transposed bufferlist
        auto res = root_builder.Finish() ;
        builder.Finish( res ) ;
        const char* fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
        int bufsz      = builder.GetSize() ;
        librados::bufferlist bl ;
        bl.append( fb, bufsz ) ;
        ::encode( bl, transposed_bl_seq ) ;
      }

      // =============================== //
      // column of strings
      else if( (unsigned)rows_data_type->Get(i) == Tables::Data_StringData ) {
        auto string_col_data = static_cast< const Tables::StringData* >( rows_data->Get(i) ) ;
      }

      // =============================== //
      else {
        std::cout << "execute_transpose: wut???" << std::endl ; 
      }
    }
  } // Relation_Rows

  else if( data_type == Tables::Relation_Col ) {

//    // make this general
//    //int counter = 0 ;
//    //while(  it_wrapped.get_remaining() > 0 ) {
//    //  std::cout << ">>>"  << it_wrapped.get_remaining() << std::endl ;
//    //  ceph::bufferlist bl ;
//    //  ::decode( bl, it_wrapped ) ;
//    //  const char* fb = bl.c_str() ;
//    //  counter += 1 ;
//    //}
//
//    // unpack int column buflist
//    ceph::bufferlist bl0 ;
//    ::decode( bl0, it_wrapped ) ;
//    const char* fb0 = bl0.c_str() ;
//    auto int_root = Tables::GetCols_int( fb0 ) ;
//
//    // unpack float column buflist
//    ceph::bufferlist bl1 ;
//    ::decode( bl1, it_wrapped ) ;
//    const char* fb1 = bl1.c_str() ;
//    auto float_root = Tables::GetCols_float( fb1 ) ;
//
//    auto rids_read   = int_root->RIDs() ;
//    auto ints_read   = int_root->data() ;
//    auto floats_read = float_root->data() ;
//
//    std::vector< uint64_t > rids_vect ; 
//    for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
//      rids_vect.push_back( rids_read->Get( i ) ) ;
//    }
//    std::vector< uint64_t > ints_vect ; 
//    for( unsigned int i = 0; i < ints_read->Length(); i++ ) {
//      ints_vect.push_back( ints_read->Get( i ) ) ;
//    }
//    std::vector< float > floats_vect ; 
//    for( unsigned int i = 0; i < floats_read->Length(); i++ ) {
//      floats_vect.push_back( floats_read->Get( i ) ) ;
//    }
//
//    // ----------------------------------------------- //
//    // build row flatbuffer
//
//    flatbuffers::FlatBufferBuilder builder( 1024 ) ;
//
//    //place these before record_builder declare
//    auto rids_vect_fb   = builder.CreateVector( rids_vect ) ;
//    auto ints_vect_fb   = builder.CreateVector( ints_vect ) ;
//    auto floats_vect_fb = builder.CreateVector( floats_vect ) ;
//    auto layout         = builder.CreateString( "ROW" ) ;
//
//    Tables::RowsBuilder row_builder( builder ) ;
//    row_builder.add_layout( layout ) ;
//    row_builder.add_RIDs( rids_vect_fb ) ;
//    row_builder.add_att0( ints_vect_fb ) ;
//    row_builder.add_att1( floats_vect_fb ) ;
//
//    auto done = row_builder.Finish() ;
//    builder.Finish( done ) ;
//
//    // save record in bufferlist bl
//    const char* row_fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;
//
//    int row_bfsz = builder.GetSize() ;
//    librados::bufferlist bl_rows ;
//    bl_rows.append( row_fb, row_bfsz ) ;
//
//    // append to transposed_bl_seq bufferlist
//    ::encode( bl_rows, transposed_bl_seq ) ;
  } // Relation_Cols

  else {
    std::cout << "lol wut? unrecognized layout" << std::endl ;
  }

  return transposed_bl_seq ;
}
