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
#include <typeinfo>

int process_fb_union( flatbuffers::FlatBufferBuilder& res_builder,
                      ceph::bufferlist wrapped_bl_seq,
                      std::vector<std::string> attlist ) {

  std::cout << "in process_fb_union..." << std::endl ;

  ceph::bufferlist::iterator it_wrapped_tmp = wrapped_bl_seq.begin() ;
  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
  std::vector< uint64_t > rids_vect ;
  std::vector< uint8_t > root_schema ;
  std::vector< flatbuffers::Offset< Tables::Record > > row_records ;
  std::vector< flatbuffers::Offset< flatbuffers::String > > rows_schema ;
  uint64_t ncols = 0 ;
  uint64_t nrows = 0 ;
  flatbuffers::Offset<flatbuffers::String> table_name ;
  std::vector< uint8_t > record_data_type_vect ;

  //Col only
  bool col_flag = false ;
  //std::vector< flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<void> > > > data_vect_vect ;
  std::vector< std::vector< flatbuffers::Offset<void> > > col_rows_vect ;
  std::vector< std::vector< std::string > > col_rows_vect_tmp ;

  // get nrows
  ceph::bufferlist bl ;
  ::decode( bl, it_wrapped_tmp ) ;
  const char* fb = bl.c_str() ;
  auto root      = Tables::GetRoot( fb ) ;
  auto data_type = root->relationData_type() ;
  if( data_type == Tables::Relation_Rows ) {
    auto rows      = static_cast< const Tables::Rows* >( root->relationData() ) ;
    nrows = rows->nrows() ;
  }
  else if( data_type == Tables::Relation_Col ) {
    auto col       = static_cast< const Tables::Col* >( root->relationData() ) ;
    nrows = col->nrows() ;
  }
  else {
    std::cout << "unrecognized data_type '" << data_type << "'" << std::endl ;
  }

  // prime row collection vector
  for( unsigned int i = 0; i < nrows; i++ ) {
    std::vector< flatbuffers::Offset<void> > dvect ;
    col_rows_vect.push_back( dvect ) ;
    std::cout << "saved an empty dvect" << std::endl ;

    std::vector< std::string > dvect_str ;
    col_rows_vect_tmp.push_back( dvect_str ) ;
    //col_rows_vect_tmp[i].push_back( "thing" ) ;
  }
  for( unsigned int i = 0; i < nrows; i++ ) {
    std::cout << "  col_rows_vect_tmp[" << i << "] = " 
              << col_rows_vect_tmp[i] << std::endl ;
  }
  // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

  // ================================================================================ //
  // display data

  int it = 0 ;
  while( it_wrapped.get_remaining() > 0 ) {

    std::vector< uint64_t > col_ints ;
    std::vector< float > col_floats ;
    std::vector< std::string > col_strs ;
    std::vector< std::string > data_vect_tmp ;

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

      // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
      // ncols
      if( attlist.size() > 0 )
        ncols = attlist.size() ;
      else
        ncols = 0 ; //increment in while loop
      // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

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

      // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
      table_name = res_builder.CreateString( table_name_read->str() ) ;
      for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
        rids_vect.push_back( rids_read->Get(i) ) ;
      }
      nrows = nrows_read ;
      // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

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

        // record data
        std::vector< flatbuffers::Offset< void > > row_data_vect ;

        for( unsigned int j = 0; j < curr_rec_data->Length(); j++ ) {
          // skip this iteration if the column num is not projected
          // or return data from all columns if no select atts provided.
          if( std::find( att_inds.begin(), att_inds.end(), j ) == att_inds.end() && att_inds.size() > 0 ) continue ;
          else {
            if(i==0)
              rows_schema.push_back( res_builder.CreateString( schema_read->Get(j)->str() ) ) ;
            // column of ints
            if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_IntData ) {
              //std::cout << "int" << "\t" ;
              auto int_col_data = static_cast< const Tables::IntData* >( curr_rec_data->Get(j) ) ;
              std::cout << int_col_data->data()->Get(0) << "\t" ;
              std::vector< uint64_t > single_iv ;
              single_iv.push_back( int_col_data->data()->Get(0) ) ;
              auto int_vect_fb = res_builder.CreateVector( single_iv ) ;
              auto iv = Tables::CreateIntData( res_builder, int_vect_fb ) ;
              row_data_vect.push_back( iv.Union() ) ;
              if(i==0) {
                record_data_type_vect.push_back( Tables::Data_IntData ) ;
                root_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
              }
            }
            // column of floats
            else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_FloatData ) {
              //std::cout << "float" << "\t" ;
              auto float_col_data = static_cast< const Tables::FloatData* >( curr_rec_data->Get(j) ) ;
              std::cout << float_col_data->data()->Get(0) << "\t" ;
              std::vector< float > single_fv ;
              single_fv.push_back( float_col_data->data()->Get(0) ) ;
              auto float_vect_fb = res_builder.CreateVector( single_fv ) ;
              auto fv = Tables::CreateFloatData( res_builder, float_vect_fb ) ;
              row_data_vect.push_back( fv.Union() ) ;
              if(i==0) {
                record_data_type_vect.push_back( Tables::Data_FloatData ) ;
                root_schema.push_back( (uint8_t)1 ) ; // 1 --> float
              }
            }
            // column of strings
            else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_StringData ) {
              //std::cout << "str" << "\t" ;
              auto string_col_data = static_cast< const Tables::StringData* >( curr_rec_data->Get(j) ) ;
              std::cout << string_col_data->data()->Get(0)->str() << "\t" ;
              std::vector< flatbuffers::Offset<flatbuffers::String> > single_sv ;
              single_sv.push_back( res_builder.CreateString( string_col_data->data()->Get(0)->str() ) ) ;
              auto string_vect_fb = res_builder.CreateVector( single_sv ) ;
              auto sv = Tables::CreateStringData( res_builder, string_vect_fb ) ;
              row_data_vect.push_back( sv.Union() ) ;
              if(i==0) {
                record_data_type_vect.push_back( Tables::Data_StringData ) ;
                root_schema.push_back( (uint8_t)2 ) ; // 2 --> string
              }
            }
            else {
              std::cout << "execute_query: unrecognized row_data_type " 
                        << (unsigned)curr_rec_data_type->Get(j) << std::endl ; 
            } //ELSE
          } //ELSE
        } //FOR
        std::cout << std::endl ;

        // save flatbuf record
        auto data              = res_builder.CreateVector( row_data_vect ) ;
        auto record_data_types = res_builder.CreateVector( record_data_type_vect ) ;
        auto rec               = Tables::CreateRecord( res_builder, record_data_types, data ) ;
        row_records.push_back( rec ) ;

      } //FOR

    } // Relation_Rows 

    // process one Root>Col flatbuffer
    else if( data_type == Tables::Relation_Col ) {
      std::cout << "else if data_type == Tables::Relation_Col" << std::endl ;
      col_flag = true ;

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

      // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
      //table_name = res_builder.CreateString( table_name_read->str() ) ;
      table_name = res_builder.CreateString( "atable" ) ;
      for( unsigned int i = 0; i < rids_read->Length(); i++ ) {
        rids_vect.push_back( rids_read->Get(i) ) ;
      }
      nrows = nrows_read ;
      // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

      // skip columns not in attlist
      if( std::find( attlist.begin(), attlist.end(), col_name_read->str() ) == attlist.end() && 
          attlist.size() > 0 ) 
        continue ;
      else {
        // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
        rows_schema.push_back( res_builder.CreateString( col_name_read->str() ) ) ;
        ncols += 1 ;
        // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

        // print data to stdout
        for( unsigned int i = 0; i < nrows_read; i++ ) {
          std::cout << rids_read->Get(i) << ":\t" ;
          // column of ints
          if( (unsigned)col_data_type == Tables::Data_IntData ) {
            auto int_col_data = static_cast< const Tables::IntData* >( col_data ) ;
            std::cout << int_col_data->data()->Get(i) << "\t" ;
            uint64_t blah_int = int_col_data->data()->Get(i) ;
            data_vect_tmp.push_back( std::to_string(blah_int) ) ;

            // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
            col_ints.push_back( int_col_data->data()->Get(i) ) ;
            std::vector< uint64_t > single_iv ;
            single_iv.push_back( int_col_data->data()->Get(i) ) ;
            auto int_vect_fb = res_builder.CreateVector( single_iv ) ;
            auto iv = Tables::CreateIntData( res_builder, int_vect_fb ) ;
            //data_vect.push_back( iv.Union() ) ;
            col_rows_vect[i].push_back( iv.Union() ) ;
            col_rows_vect_tmp[i].push_back( std::to_string(blah_int) ) ;
            std::cout << "  saved int " << std::to_string(int_col_data->data()->Get(i)) << std::endl ;
            if( i==0 ) {
              record_data_type_vect.push_back( Tables::Data_IntData ) ;
              root_schema.push_back( (uint8_t)0 ) ; // 0 --> uint64
            }
            // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
          }
          // column of floats
          else if( (unsigned)col_data_type == Tables::Data_FloatData ) {
            auto float_col_data = static_cast< const Tables::FloatData* >( col_data ) ;
            std::cout << float_col_data->data()->Get(i) << "\t" ;
            float blah_float = float_col_data->data()->Get(i) ;
            data_vect_tmp.push_back( std::to_string(blah_float) ) ;

            // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
            col_floats.push_back( float_col_data->data()->Get(i) ) ;
            std::vector< float > single_fv ;
            single_fv.push_back( float_col_data->data()->Get(i) ) ;
            auto float_vect_fb = res_builder.CreateVector( single_fv ) ;
            auto fv = Tables::CreateFloatData( res_builder, float_vect_fb ) ;
            //data_vect.push_back( fv.Union() ) ;
            col_rows_vect[i].push_back( fv.Union() ) ;
            col_rows_vect_tmp[i].push_back( std::to_string(blah_float) ) ;
            std::cout << "  saved float " << std::to_string(float_col_data->data()->Get(i)) << std::endl ;
            if( i==0 ) {
              record_data_type_vect.push_back( Tables::Data_FloatData ) ;
              root_schema.push_back( (uint8_t)1 ) ; // 1 --> float
            }
            // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
          }
          // column of strings
          else if( (unsigned)col_data_type == Tables::Data_StringData ) {
            auto string_col_data = static_cast< const Tables::StringData* >( col_data ) ;
            std::cout << string_col_data->data()->Get(i)->str() << "\t" ;
            std::string blah_string = string_col_data->data()->Get(i)->str() ;
            data_vect_tmp.push_back( blah_string ) ;

            // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
            col_strs.push_back( string_col_data->data()->Get(i)->str() ) ;
            std::vector< flatbuffers::Offset<flatbuffers::String> > single_sv ;
            single_sv.push_back( res_builder.CreateString( string_col_data->data()->Get(i)->str() ) ) ;
            auto string_vect_fb = res_builder.CreateVector( single_sv ) ;
            auto sv = Tables::CreateStringData( res_builder, string_vect_fb ) ;
            //data_vect.push_back( sv.Union() ) ;
            col_rows_vect[i].push_back( sv.Union() ) ;
            col_rows_vect_tmp[i].push_back( blah_string ) ;
            std::cout << "  saved str " << string_col_data->data()->Get(i)->str() << std::endl ;
            if( i==0 ) {
              record_data_type_vect.push_back( Tables::Data_StringData ) ;
              root_schema.push_back( (uint8_t)2 ) ; // 2 --> string
            }
            // fb stuff ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
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

    if( col_flag ) {
      std::cout << "data_vect_tmp = " << std::endl ;
      std::cout << data_vect_tmp << std::endl ;
      //auto data = res_builder.CreateVector( data_vect ) ;
      //data_vect_vect.push_back( data ) ;
      std::cout << "col_rows_vect_tmp.size() = " << col_rows_vect_tmp.size() << std::endl ;
      for( unsigned int i = 0; i < col_rows_vect_tmp.size(); i++ ) {
        std::cout << "col_rows_vect_tmp[" << i << "] = " ;
        std::cout << col_rows_vect_tmp[i] ;
        std::cout << ", .size() = " << col_rows_vect_tmp[i].size() << std::endl ;
      }
    }

    it++ ;
  } // while

  // --------------------------------------------- //
  // save out as Rows flatbuffer
  //
  //   table Rows {
  //     skyhook_version : ubyte ;  // skyhook version
  //     schema_version  : ubyte ;
  //   
  //     table_name : string     ;
  //     schema     : [ string ] ;
  //     nrows      : uint64     ;
  //     ncols      : uint64     ;
  //     layout     : string     ;
  //   
  //     RIDs : [ uint64 ] ;
  //     data : [ Record ] ;
  //   }
  //

  std::cout << "----- ouside while loop -----" << std::endl ;

  if( col_flag ) {
    std::cout << "--col_flag is true--"<< std::endl ;
    auto record_data_types = res_builder.CreateVector( record_data_type_vect ) ;
    // save flatbuf records
    for( unsigned int i = 0; i < col_rows_vect.size(); i++ ) {
      auto row_vect = res_builder.CreateVector( col_rows_vect[i] ) ;
      auto rec = Tables::CreateRecord( res_builder, record_data_types, row_vect ) ;
      row_records.push_back( rec ) ;
      std::cout << "  >saved rec" << std::endl ;
    }
    std::cout << "row_records.size() = " << row_records.size() << std::endl ;
  }

  //place these before record_builder declare
  auto layout         = res_builder.CreateString( "ROW" ) ;
  auto schema_fb      = res_builder.CreateVector( rows_schema ) ;
  auto rids_vect_fb   = res_builder.CreateVector( rids_vect ) ;
  auto row_records_fb = res_builder.CreateVector( row_records ) ;
  auto root_schema_fb = res_builder.CreateVector( root_schema ) ;

  // create the Rows flatbuffer:
  auto rows = Tables::CreateRows(
    res_builder,
    0,
    0,
    table_name,
    schema_fb,
    nrows,
    ncols,
    layout,
    rids_vect_fb,
    row_records_fb ) ;

  Tables::RootBuilder root_builder( res_builder ) ;
  root_builder.add_schema( root_schema_fb ) ;

  // save the Rows flatbuffer to the root flatbuffer
  root_builder.add_relationData_type( Tables::Relation_Rows ) ;
  root_builder.add_relationData( rows.Union() ) ;

  auto res = root_builder.Finish() ;
  res_builder.Finish( res ) ;

  return 0 ;
}
