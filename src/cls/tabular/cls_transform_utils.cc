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

  while( it_wrapped.get_remaining() > 0 ) {

    // grab the Root
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* fb = bl.c_str() ;

    auto root      = Tables::GetRoot( fb ) ;
    auto data_type = root->relationData_type() ;

    std::cout << "data_type : " << data_type << std::endl ;

    // process one Root>Rows flatbuffer
    if( data_type == Tables::Relation_Rows ) {
      std::cout << "if data_type == Tables::Relation_Rows" << std::endl ;

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
      auto rows_data       = rows->data() ; // [ Record ]

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
      for( unsigned int i = 0; i < rows_data->Length(); i++ ) {
        std::cout << rids_read->Get(i) << ":\t" ;

        auto curr_rec           = rows_data->Get(i) ;
        auto curr_rec_data      = curr_rec->data() ;
        auto curr_rec_data_type = curr_rec->data_type() ;

        for( unsigned int j = 0; j < curr_rec_data->Length(); j++ ) {
          // column of ints
          if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_IntData ) {
            //std::cout << "int" << "\t" ;
            auto int_col_data = static_cast< const Tables::IntData* >( curr_rec_data->Get(j) ) ;
            std::cout << int_col_data->data()->Get(0) << "\t" ;
          }
          // column of floats
          else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_FloatData ) {
            //std::cout << "float" << "\t" ;
            auto float_col_data = static_cast< const Tables::FloatData* >( curr_rec_data->Get(j) ) ;
            std::cout << float_col_data->data()->Get(0) << "\t" ;
          }
          // column of strings
          else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_StringData ) {
            //std::cout << "str" << "\t" ;
            auto string_col_data = static_cast< const Tables::StringData* >( curr_rec_data->Get(j) ) ;
            std::cout << string_col_data->data()->Get(0)->str() << "\t" ;
          }
          else {
            std::cout << "execute_query: unrecognized row_data_type " 
                      << (unsigned)curr_rec_data_type->Get(j) << std::endl ; 
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
          std::cout << string_col_data->data()->Get(i)->str() << "\t" ;
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
  } // while

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

  librados::bufferlist transformed_bl_seq ;
  switch( t_op.transform_type ) {
    case 0 :
      transformed_bl_seq = transpose( wrapped_bl_seq ) ;
      break ;
    default :
      std::cout << "execute_transform: unrecognized t_op.transform_type '" << t_op.transform_type << "'" << std::endl ;
      break ;
  }

  // write bl_seq to ceph object
  auto obj_oid = t_op.oid + "_transposed" ;
  const char *obj_name  = obj_oid.c_str() ;
  bufferlist::iterator p = transformed_bl_seq.begin();
  size_t i = p.get_remaining() ;
  ret = ioctx.write( obj_name, transformed_bl_seq, i, 0 ) ;
  checkret( ret, 0 ) ;

//// ------------------- \/\/\/ EXPERIMENTAL \/\/\/ ------------------- //
//  // define transform operation for re-composition
////  transform_op to1 ;
////  to1.oid            = "blah2_transposed" ;
////  to1.pool           = "tpchflatbuf" ;
////  to1.table_name     = "atable" ;
////  to1.transform_type = 0 ; // 0 --> transpose
////
////  auto recomposed_bl_seq = transpose( transformed_bl_seq ) ;
////
////  // write bl_seq to ceph object
////  auto obj_oid1 = to1.oid + "_transposed" ;
////  const char *obj_name1  = obj_oid1.c_str() ;
////  bufferlist::iterator p1 = recomposed_bl_seq.begin();
////  size_t i1 = p1.get_remaining() ;
////  ret = ioctx.write( obj_name1, recomposed_bl_seq, i1, 0 ) ;
////  checkret( ret, 0 ) ;
//// ------------------- /\/\/\ EXPERIMENTAL /\/\/\  ------------------- //

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
  auto data_type = root->relationData_type() ;
  std::cout << "data_type : " << data_type << std::endl ;

  // process ROWs
  if( data_type == Tables::Relation_Rows ) {
    std::cout << "transpose: data_type == Tables::Relation_Col" << std::endl ;

    auto rows = static_cast< const Tables::Rows* >( root->relationData() ) ;
    auto table_name_read = rows->table_name() ;
    auto schema_read     = rows->schema() ;
    auto nrows_read      = rows->nrows() ;
    auto ncols_read      = rows->ncols() ;
    auto rids_read       = rows->RIDs() ;
    auto rows_data       = rows->data() ; // [ Record ]

    std::cout << "table_name_read->str() : " << table_name_read->str() << std::endl ;
    std::cout << "nrows_read     : " << nrows_read     << std::endl ;
    std::cout << "ncols_read     : " << ncols_read     << std::endl ;

    std::vector< uint64_t > RIDs_vect ;
    for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
      RIDs_vect.push_back( rids_read->Get(i) ) ;
    }

    // build out Root>Col
    flatbuffers::FlatBufferBuilder builder( 1024 ) ;
    auto RIDs = builder.CreateVector( RIDs_vect ) ;

    // get schema from first record in Rows
    auto init_rec = rows_data->Get(0) ;
    auto schema   = init_rec->data_type() ;

    for( unsigned int i = 0; i < schema->Length(); i++ ) {
      std::cout << "  schema: " << (unsigned)schema->Get(i) << std::endl ;

      if( (unsigned)schema->Get(i) == Tables::Data_IntData ) {

        std::vector< uint64_t > iv ;

        // extract all of the ith records into an IntData list
        for( unsigned j = 0; j < rows_data->Length(); j++ ) {
          auto curr_rec           = rows_data->Get(j) ;
          auto curr_rec_data      = curr_rec->data() ;

          auto int_col_data_read = static_cast< const Tables::IntData* >( curr_rec_data->Get(i) ) ;
          iv.push_back( int_col_data_read->data()->Get(0) ) ; // assume list of size 1
        }
        auto iv_fb = builder.CreateVector( iv ) ;
        auto int_col_data = Tables::CreateIntData( builder, iv_fb ) ;
        auto col_name = builder.CreateString( schema_read->Get(i)->str() ) ;

        auto col = CreateCol(
          builder,
          0,
          0,
          col_name,
          (uint8_t)i,
          nrows_read,
          RIDs,
          Tables::Data_IntData,
          int_col_data.Union() ) ;

        // save the Rows flatbuffer to the root flatbuffer
        Tables::RootBuilder root_builder( builder ) ;
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
      else if( (unsigned)schema->Get(i) == Tables::Data_FloatData ) {

        std::vector< float > fv ;

        // extract all of the ith records into an IntData list
        for( unsigned j = 0; j < rows_data->Length(); j++ ) {
          auto curr_rec           = rows_data->Get(j) ;
          auto curr_rec_data      = curr_rec->data() ;

          auto float_col_data_read = static_cast< const Tables::FloatData* >( curr_rec_data->Get(i) ) ;
          fv.push_back( float_col_data_read->data()->Get(0) ) ; // assume list of size 1
        }
        auto fv_fb = builder.CreateVector( fv ) ;
        auto float_col_data = Tables::CreateFloatData( builder, fv_fb ) ;
        auto col_name = builder.CreateString( schema_read->Get(i)->str() ) ;

        auto col = CreateCol(
          builder,
          0,
          0,
          col_name,
          (uint8_t)i,
          nrows_read,
          RIDs,
          Tables::Data_FloatData,
          float_col_data.Union() ) ;

        // save the Rows flatbuffer to the root flatbuffer
        Tables::RootBuilder root_builder( builder ) ;
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
      else if( (unsigned)schema->Get(i) == Tables::Data_StringData ) {

        std::vector< flatbuffers::Offset<flatbuffers::String> > sv ;

        // extract all of the ith records into an IntData list
        for( unsigned j = 0; j < rows_data->Length(); j++ ) {
          auto curr_rec           = rows_data->Get(j) ;
          auto curr_rec_data      = curr_rec->data() ;

          auto string_col_data_read = static_cast< const Tables::StringData* >( curr_rec_data->Get(i) ) ;
          sv.push_back( builder.CreateString( string_col_data_read->data()->Get(0) ) ) ; // assume list of size 1
        }
        auto sv_fb = builder.CreateVector( sv ) ;
        auto string_col_data = Tables::CreateStringData( builder, sv_fb ) ;
        auto col_name = builder.CreateString( schema_read->Get(i)->str() ) ;

        auto col = CreateCol(
          builder,
          0,
          0,
          col_name,
          (uint8_t)i,
          nrows_read,
          RIDs,
          Tables::Data_StringData,
          string_col_data.Union() ) ;

        // save the Rows flatbuffer to the root flatbuffer
        Tables::RootBuilder root_builder( builder ) ;
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
      else {
        std::cout << "transpose : unrecognized data_type " 
                  << (unsigned)schema->Get(i) << std::endl ;
      }
    }

    for( unsigned int i = 0; i < rows_data->Length(); i++ ) {
      std::cout << rids_read->Get(i) << ":\t" ;

      auto curr_rec           = rows_data->Get(i) ;
      auto curr_rec_data      = curr_rec->data() ;
      auto curr_rec_data_type = curr_rec->data_type() ;

      for( unsigned int j = 0; j < curr_rec_data->Length(); j++ ) {
        // column of ints
        if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_IntData ) {
          //std::cout << "int" << "\t" ;
          auto int_col_data = static_cast< const Tables::IntData* >( curr_rec_data->Get(j) ) ;
          std::cout << int_col_data->data()->Get(0) << "\t" ;
        }
        // column of floats
        else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_FloatData ) {
          //std::cout << "float" << "\t" ;
          auto float_col_data = static_cast< const Tables::FloatData* >( curr_rec_data->Get(j) ) ;
          std::cout << float_col_data->data()->Get(0) << "\t" ;
        }
        // column of strings
        else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_StringData ) {
          //std::cout << "str" << "\t" ;
          auto string_col_data = static_cast< const Tables::StringData* >( curr_rec_data->Get(j) ) ;
          std::cout << string_col_data->data()->Get(0)->str() << "\t" ;
        }
        else {
          std::cout << "execute_query: unrecognized row_data_type " 
                    << (unsigned)curr_rec_data_type->Get(j) << std::endl ; 
        }
      }
      std::cout << std::endl ;
    }
  } // Relation_Rows

  else if( data_type == Tables::Relation_Col ) {
    std::cout << "transpose: data_type == Tables::Relation_Col" << std::endl ; 
/*
    // make this general
    //int counter = 0 ;
    //while(  it_wrapped.get_remaining() > 0 ) {
    //  std::cout << ">>>"  << it_wrapped.get_remaining() << std::endl ;
    //  ceph::bufferlist bl ;
    //  ::decode( bl, it_wrapped ) ;
    //  const char* fb = bl.c_str() ;
    //  counter += 1 ;
    //}

    // unpack int column buflist
    ceph::bufferlist bl0 ;
    ::decode( bl0, it_wrapped ) ;
    const char* fb0 = bl0.c_str() ;
    auto int_root = Tables::GetCols_int( fb0 ) ;

    // unpack float column buflist
    ceph::bufferlist bl1 ;
    ::decode( bl1, it_wrapped ) ;
    const char* fb1 = bl1.c_str() ;
    auto float_root = Tables::GetCols_float( fb1 ) ;

    auto rids_read   = int_root->RIDs() ;
    auto ints_read   = int_root->data() ;
    auto floats_read = float_root->data() ;

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
    // build row flatbuffer

    flatbuffers::FlatBufferBuilder builder( 1024 ) ;

    //place these before record_builder declare
    auto rids_vect_fb   = builder.CreateVector( rids_vect ) ;
    auto ints_vect_fb   = builder.CreateVector( ints_vect ) ;
    auto floats_vect_fb = builder.CreateVector( floats_vect ) ;
    auto layout         = builder.CreateString( "ROW" ) ;

    Tables::RowsBuilder row_builder( builder ) ;
    row_builder.add_layout( layout ) ;
    row_builder.add_RIDs( rids_vect_fb ) ;
    row_builder.add_att0( ints_vect_fb ) ;
    row_builder.add_att1( floats_vect_fb ) ;

    auto done = row_builder.Finish() ;
    builder.Finish( done ) ;

    // save record in bufferlist bl
    const char* row_fb = reinterpret_cast<char*>( builder.GetBufferPointer() ) ;

    int row_bfsz = builder.GetSize() ;
    librados::bufferlist bl_rows ;
    bl_rows.append( row_fb, row_bfsz ) ;

    // append to transposed_bl_seq bufferlist
    ::encode( bl_rows, transposed_bl_seq ) ;
*/
  } // Relation_Cols

  else {
    std::cout << "transform: unrecognized layout" << std::endl ;
  }

  return transposed_bl_seq ;
}
