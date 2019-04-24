/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include "query_utils.h"

int process_fb_union( flatbuffers::FlatBufferBuilder& res_flatbldr,
                      ceph::bufferlist wrapped_bl_seq,
                      std::vector<std::string> attlist ) {

  std::cout << "in process_fb_union..." << std::endl ;

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

      // get indexes of select atts
      std::vector< int > att_inds ;
      for( unsigned int i = 0; i < schema_read->Length(); i++ ) {
        if( std::find( attlist.begin(), attlist.end(), schema_read->Get(i)->str() ) != attlist.end() )
          att_inds.push_back( i ) ;
      }

      std::cout << "att_inds:" << std::endl ;
      std::cout << att_inds << std::endl ;

      //auto int_data = static_cast< const Tables::IntData* >( rows_data->Get(0) ) ;
      //std::cout << int_data->data()->Get(0) << std::endl ;
      // print data to stdout
      for( unsigned int i = 0; i < rows_data->Length(); i++ ) {

        std::cout << rids_read->Get(i) << ":\t" ;

        auto curr_rec           = rows_data->Get(i) ;
        auto curr_rec_data      = curr_rec->data() ;
        auto curr_rec_data_type = curr_rec->data_type() ;

        for( unsigned int j = 0; j < curr_rec_data->Length(); j++ ) {
          // skip this iteration if the column num is not projected
          // or return data from all columns if no select atts provided.
          if( std::find( att_inds.begin(), att_inds.end(), j ) == att_inds.end() && att_inds.size() > 0 ) continue ;
          else {
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
          } //ELSE
        } //FOR
        std::cout << std::endl ;
      } //FOR
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

      // skip columns not in attlist
      if( std::find( attlist.begin(), attlist.end(), col_name_read->str() ) == attlist.end() && attlist.size() > 0 ) continue ;
      else {
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
      }
    } // Relation_Col

    else {
      std::cout << "unrecognized data_type '" << data_type << "'" << std::endl ;
    }
  } // while

  return 0 ;
}

//int process_fb_col( flatbuffers::FlatBufferBuilder& query_flatbldr,
//                    const char* table_fb ) {
//  return 0 ;
//}
