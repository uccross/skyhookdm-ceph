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
//#include <errno.h>
//#include <string>
//#include <sstream>
//#include <boost/lexical_cast.hpp>
//#include <time.h>
//#include "re2/re2.h"
//#include "include/types.h"
//#include "objclass/objclass.h"
//#include "cls_tabular_utils.h"
//#include "cls_tabular.h"


void print_stuff() {
  std::cout << "blee" << std::endl ;
}

// ============================== //
//          EXECUTE QUERY         //
// ============================== //
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
} // EXECUTE QUERY


// ================================= //
//         EXECUTE TRANSFORM         //
// ================================= //
void execute_transform( transform_op t_op ) {
  std::cout << "in execute_transform..." << std::endl ;
  std::cout << "t_op.oid            = " << t_op.oid            << std::endl ;
  std::cout << "t_op.pool           = " << t_op.pool           << std::endl ;
  std::cout << "t_op.table_name     = " << t_op.table_name     << std::endl ;
  std::cout << "t_op.transform_type = " << t_op.transform_type << std::endl ;
  std::cout << "t_op.bloffs         = " << t_op.bloffs         << std::endl ;

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
      transformed_bl_seq = transpose( wrapped_bl_seq, t_op ) ;
      break ;
    default :
      std::cout << "execute_transform: unrecognized t_op.transform_type '" 
                << t_op.transform_type << "'" << std::endl ;
      break ;
  }

  // write bl_seq to ceph object
  auto obj_oid = t_op.oid + "_transposed" ;
  const char *obj_name  = obj_oid.c_str() ;
  bufferlist::iterator p = transformed_bl_seq.begin();
  size_t i = p.get_remaining() ;
  ret = ioctx.write( obj_name, transformed_bl_seq, i, 0 ) ;
  checkret( ret, 0 ) ;

  ioctx.close() ;
} // EXECUTE TRANSFORM


// ========================== //
//         TRANSPOSE          //
// ========================== //
librados::bufferlist transpose( librados::bufferlist wrapped_bl_seq, transform_op to ) {
  std::cout << "in transpose..." << std::endl ;

  librados::bufferlist transposed_bl_seq ;
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // need to prime structures for Col to Rows transpose
  std::vector< uint8_t > rows_meta_schema ;
  std::vector< flatbuffers::Offset< flatbuffers::String > > rows_schema ;
  std::vector< uint64_t > rows_rids_vect ;
  std::vector< flatbuffers::Offset< Tables::Record > > rows_row_records ;
  std::vector< uint8_t > rows_record_data_type_vect ;
  flatbuffers::FlatBufferBuilder rows_builder( 1024 ) ;
  uint8_t rows_nrows ;
  uint8_t rows_ncols = to.bloffs.size() ;
  std::vector< std::vector< uint64_t > > all_ints ;
  std::vector< std::vector< float > > all_floats ;
  std::vector< std::vector< std::string > > all_strs ;

  int counter = 0 ;
  while( it_wrapped.get_remaining() > 0 ) {

    std::vector< uint64_t > col_ints ;
    std::vector< float > col_floats ;
    std::vector< std::string > col_strs ;

    for( unsigned int i = 0; i < to.bloffs.size(); i++ ) {

      // skip iterator processing at offsets not included in bloffs
      if( std::find( to.bloffs.begin(), to.bloffs.end(), i ) == to.bloffs.end() ) {
        counter++ ;
        continue ;
      }

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

              // assume list of size 1
              sv.push_back( builder.CreateString( string_col_data_read->data()->Get(0) ) ) ;
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

        // unpack Col
        auto col = static_cast< const Tables::Col* >( root->relationData() ) ;
        auto col_name_read  = col->col_name() ;
        auto col_index_read = col->col_name() ;
        auto nrows_read     = col->nrows() ;
        auto rids_read      = col->RIDs() ;
        auto col_data_type  = col->data_type() ; // type of Data
        auto col_data       = col->data() ; // Data
    
        std::cout << "col_name_read->str()  : " << col_name_read->str() << std::endl ;
        std::cout << "col_index_read->str() : " << col_index_read->str() << std::endl ;
        std::cout << "nrows_read            : " << (unsigned)nrows_read  << std::endl ;
    
        if( counter == 0 ) {
          rows_nrows = nrows_read ;

          // all RIDs should be the same
          for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
            rows_rids_vect.push_back( rids_read->Get(i) ) ;
          }

          // create initial Records for each row
          if( (unsigned)col_data_type == Tables::Data_IntData ) {
            auto col_data_read = static_cast< const Tables::IntData* >( col_data ) ;
            for( unsigned int i = 0; i < col_data_read->data()->Length(); i++ ) {
              std::cout << col_data_read->data()->Get(i) << std::endl ;
              col_ints.push_back( col_data_read->data()->Get(i) ) ;
            }
            rows_meta_schema.push_back( (unsigned)0 ) ;
          }
          else if( (unsigned)col_data_type == Tables::Data_FloatData ) {
            auto col_data_read = static_cast< const Tables::FloatData* >( col_data ) ;
            for( unsigned int i = 0; i < col_data_read->data()->Length(); i++ ) {
              std::cout << col_data_read->data()->Get(i) << std::endl ;
              col_floats.push_back( col_data_read->data()->Get(i) ) ;
            }
            rows_meta_schema.push_back( (unsigned)1 ) ;
          }
          else if( (unsigned)col_data_type == Tables::Data_StringData ) {
            auto col_data_read = static_cast< const Tables::StringData* >( col_data ) ;
            for( unsigned int i = 0; i < col_data_read->data()->Length(); i++ ) {
              std::cout << col_data_read->data()->Get(i)->str() << std::endl ;
              col_strs.push_back( col_data_read->data()->Get(i)->str() ) ;
            }
            rows_meta_schema.push_back( (unsigned)2 ) ;
          }
          else {
            std::cout << "transpose: unrecognized column data type " 
                      << (unsigned)col_data_type << std::endl ;
          }
        }

        std::cout << "add to rows_meta_schema" << std::endl ;
        std::cout << "add to rows_schema" << std::endl ;
        std::cout << "add to rows_record_data_type_vect" << std::endl ;
        rows_schema.push_back( rows_builder.CreateString( col_name_read ) ) ;
        rows_record_data_type_vect.push_back( col_data_type ) ;

      } // Relation_Cols
    
      else {
        std::cout << "transform: unrecognized layout" << std::endl ;
      }
    } // FOR

    all_ints.push_back( col_ints ) ;
    all_floats.push_back( col_floats ) ;
    all_strs.push_back( col_strs ) ;

    counter++ ;
  } // WHILE

  if( to.layout == 1 ) {
    auto rows_table_name        = rows_builder.CreateString( "atable" ) ;
    auto rows_layout            = rows_builder.CreateString( "ROW" ) ;
    auto rows_schema_fb         = rows_builder.CreateVector( rows_schema ) ;
    auto rows_rids_vect_fb      = rows_builder.CreateVector( rows_rids_vect ) ;
    auto a                      = rows_builder.CreateVector( rows_meta_schema ) ;
    auto rows_record_data_types = rows_builder.CreateVector( rows_record_data_type_vect ) ;

    // this process destroys column ordering from original
    // if the relation has multiple columns of the same type
    for( unsigned int i = 0; i < rows_nrows; i++ ) {

      std::vector< uint64_t > single_iv ;
      std::vector< float > single_fv ;
      std::vector< flatbuffers::Offset<flatbuffers::String> > single_sv ;

      //grab the ints
      for( unsigned int j = 0; j < all_ints.size(); j++ ) {
        single_iv.push_back( all_ints[j][i] ) ;
      }
      //grab the floats
      for( unsigned int j = 0; j < all_floats.size(); j++ ) {
        single_fv.push_back( all_floats[j][i] ) ;
      }
      //grab the strs
      for( unsigned int j = 0; j < all_strs.size(); j++ ) {
        single_sv.push_back( rows_builder.CreateString( all_strs[j][i] ) ) ;
      }

      auto int_vect_fb    = rows_builder.CreateVector( single_iv ) ;
      auto float_vect_fb  = rows_builder.CreateVector( single_fv ) ;
      auto string_vect_fb = rows_builder.CreateVector( single_sv ) ;
      auto iv = Tables::CreateIntData( rows_builder, int_vect_fb ) ;
      auto fv = Tables::CreateFloatData( rows_builder, float_vect_fb ) ;
      auto sv = Tables::CreateStringData( rows_builder, string_vect_fb ) ;

      std::vector< flatbuffers::Offset< void > > data_vect ;
      data_vect.push_back( iv.Union() ) ;
      data_vect.push_back( fv.Union() ) ;
      data_vect.push_back( sv.Union() ) ;
      auto data = rows_builder.CreateVector( data_vect ) ;

      auto rec = Tables::CreateRecord( rows_builder, rows_record_data_types, data ) ;
      rows_row_records.push_back( rec ) ;
    }

    auto rows_row_records_fb = rows_builder.CreateVector( rows_row_records ) ;

    // create the Rows flatbuffer:
    auto rows = Tables::CreateRows(
      rows_builder,
      0,
      0,
      rows_table_name,
      rows_schema_fb,
      rows_nrows,
      rows_ncols,
      rows_layout,
      rows_rids_vect_fb,
      rows_row_records_fb ) ;
    
    Tables::RootBuilder root_builder( rows_builder ) ;
    root_builder.add_schema( a ) ;
 
    // save the Rows flatbuffer to the root flatbuffer
    root_builder.add_relationData_type( Tables::Relation_Rows ) ;
    root_builder.add_relationData( rows.Union() ) ;

    auto res = root_builder.Finish() ;
    rows_builder.Finish( res ) ;

    const char* fb = reinterpret_cast<char*>( rows_builder.GetBufferPointer() ) ;
    int bufsz      = rows_builder.GetSize() ;
    librados::bufferlist bl ;
    bl.append( fb, bufsz ) ;

    // write to flatbuffer
    ::encode( bl, transposed_bl_seq ) ;

// ----SANITY CHECK--------------------------------------------------- //
//
//    ceph::bufferlist _bl ;
//    ::decode( _bl, transposed_bl_seq ) ; // this decrements get_remaining by moving iterator
//    const char* _fb = _bl.c_str() ;
//
//    auto _root      = Tables::GetRoot( _fb ) ;
//    auto _data_type = _root->relationData_type() ;
//
//    std::cout << "_data_type : " << _data_type << std::endl ;
//
//    auto _rows = static_cast< const Tables::Rows* >( _root->relationData() ) ;
//    auto _table_name_read = _rows->table_name() ;
//
//    auto _schema_read = _rows->schema() ;
//    auto _nrows_read  = _rows->nrows() ;
//    auto _ncols_read  = _rows->ncols() ;
//    auto _rids_read   = _rows->RIDs() ;
//    auto _rows_data   = _rows->data() ; // [ Record ]
//
//    std::cout << "_table_name_read->str() : " << _table_name_read->str() << std::endl ;
//    std::cout << "_schema_read->Length() : " << _schema_read->Length() << std::endl ;
//    std::cout << "_nrows_read     : " << _nrows_read     << std::endl ;
//    std::cout << "_ncols_read     : " << _ncols_read     << std::endl ;
//
//    // print schema to stdout
//    std::cout << "RID\t" ;
//    for( unsigned int i = 0; i < _ncols_read; i++ ) {
//      std::cout << _schema_read->Get(i)->str() << "\t" ;
//    }
//    std::cout << std::endl ;
//
//    // print data to stdout
//    for( unsigned int i = 0; i < _rows_data->Length(); i++ ) {
//      std::cout << _rids_read->Get(i) << ":\t" ;
//
//      auto _curr_rec           = _rows_data->Get(i) ;
//      auto _curr_rec_data      = _curr_rec->data() ;
//      auto _curr_rec_data_type = _curr_rec->data_type() ;
//
//      for( unsigned int j = 0; j < _curr_rec_data->Length(); j++ ) {
//        // column of ints
//        if( (unsigned)_curr_rec_data_type->Get(j) == Tables::Data_IntData ) {
//          //std::cout << "int" << "\t" ;
//          auto _int_col_data = static_cast< const Tables::IntData* >( _curr_rec_data->Get(j) ) ;
//          std::cout << _int_col_data->data()->Get(0) << "\t" ;
//        }
//        // column of floats
//        else if( (unsigned)_curr_rec_data_type->Get(j) == Tables::Data_FloatData ) {
//          //std::cout << "float" << "\t" ;
//          auto _float_col_data = static_cast< const Tables::FloatData* >( _curr_rec_data->Get(j) ) ;
//          std::cout << _float_col_data->data()->Get(0) << "\t" ;
//        }
//        // column of strings
//        else if( (unsigned)_curr_rec_data_type->Get(j) == Tables::Data_StringData ) {
//          //std::cout << "str" << "\t" ;
//          auto _string_col_data = static_cast< const Tables::StringData* >( _curr_rec_data->Get(j) ) ;
//          std::cout << _string_col_data->data()->Get(0)->str() << "\t" ;
//        }
//        else {
//          std::cout << "execute_query: unrecognized row_data_type "
//                    << (unsigned)_curr_rec_data_type->Get(j) << std::endl ;
//        }
//      }
//      std::cout << std::endl ;
//    }
// ----SANITY CHECK--------------------------------------------------- //

  }

  return transposed_bl_seq ;
} // TRANSPOSE
