
// bin/fbreader --oid atable_transposed --pool tpchflatbuf --debug yes

#include "include/types.h"
#include <string>
#include <sstream>
#include <type_traits>
#include <errno.h>
#include <time.h>
#include <boost/algorithm/string/classification.hpp> // for boost::is_any_of
#include <boost/algorithm/string/split.hpp> // for boost::split
#include <boost/algorithm/string.hpp> // for boost::trim
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>

#include "re2/re2.h"
#include "objclass/objclass.h"
#include <iostream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <condition_variable>
#include "re2/re2.h"
#include "include/rados/librados.hpp"

#include "cls_tabular_utils.h"

namespace po = boost::program_options ;

void do_read( bool, std::string, std::string ) ;
long long int printFlatbufFBUAsCSV(
        const char* dataptr,
        const size_t datasz,
        bool print_header,
        bool print_verbose,
        long long int max_to_print) ;

int main( int argc, char *argv[] ) {

  bool debug ;
  std::string oid  ;
  std::string pool ;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->required(), "debug")
    ("oid", po::value<std::string>(&oid)->required(), "oid")
    ("pool", po::value<std::string>(&pool)->required(), "pool") ;

  po::options_description all_opts( "Allowed options" ) ;
  all_opts.add( gen_opts ) ;
  po::variables_map vm ;
  po::store( po::parse_command_line( argc, argv, all_opts ), vm ) ;
  if( vm.count( "help" ) ) {
    std::cout << all_opts << std::endl ;
    return 1;
  }
  po::notify( vm ) ;

  do_read( debug, oid, pool ) ;

  return 0 ;
}

// ========== //
//   DO READ  //
// ========== //
void do_read( bool debug, 
              std::string oid,
              std::string pool ) {

  if( debug ) {
    std::cout << "in do_read..." << std::endl ;
    std::cout << "oid         : " << oid         << std::endl ;
    std::cout << "pool        : " << pool        << std::endl ;
  }

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret( ret, 0 ) ;

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( pool.c_str(), ioctx ) ;

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;

  if( debug )
    std::cout << "num_bytes_read : " << num_bytes_read << std::endl ; 

  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // ================================================================================ //
  // display data

  while( it_wrapped.get_remaining() > 0 ) {

    if( debug )
      std::cout << "it_wrapped.get_remaining() = " << it_wrapped.get_remaining() << std::endl ;

    // grab the Root
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* dataptr = bl.c_str() ;
    size_t datasz       = bl.length() ;
    bool print_header   = true ;
    bool print_verbose  = false ;
    if( debug )
      print_verbose  = true ;
    long long int max_to_print = 0 ;

    printFlatbufFBUAsCSV(
      dataptr,
      datasz,
      print_header,
      print_verbose,
      max_to_print ) ;

    if( debug )
      std::cout << "loop while" << std::endl ;

  } // while

  ioctx.close() ;

} // do_read


long long int printFlatbufFBUAsCSV(
        const char* dataptr,
        const size_t datasz,
        bool print_header,
        bool print_verbose,
        long long int max_to_print) {

  auto root       = Tables::GetRoot_FBU( dataptr ) ;
  auto nrows      = root->nrows() ;
  auto table_name = root->table_name() ;
  auto ncols      = root->ncols() ;
  auto format     = root->relationData_type() ;
  auto data       = root->relationData() ;

  if( print_verbose ) {
    std::cout << "format     : " << format << std::endl ;
    std::cout << "table_name : " << table_name->str() << std::endl ;
    std::cout << "nrows      : " << nrows << std::endl ;
    std::cout << "ncols      : " << ncols << std::endl ;
  }

  // process one Root>Rows flatbuffer
  if( format == Tables::Relation_FBU_Rows_FBU ) {
    if( print_verbose )
      std::cout << "if format == Tables::Relation_FBU_Rows_FBU" << std::endl ;

    auto rows = static_cast< const Tables::Rows_FBU* >( data ) ;
    auto rows_data = rows->data() ;

    // print data to stdout
    for( unsigned int i = 0; i < rows_data->Length(); i++ ) {

      auto curr_rec           = rows_data->Get(i) ;
      auto curr_rid           = curr_rec->RID() ;
      auto curr_rec_data      = curr_rec->data() ;
      auto curr_rec_data_type = curr_rec->data_type() ;

      if( print_verbose )
        std::cout << curr_rid << ":\t" ;

      for( unsigned int j = 0; j < curr_rec_data->Length(); j++ ) {
        switch( (unsigned)curr_rec_data_type->Get(j) ) {
          case Tables::DataTypes_FBU_SDT_UINT64_FBU : {
            auto int_col_data = static_cast< const Tables::SDT_UINT64_FBU* >( curr_rec_data->Get(j) ) ;
            std::cout << int_col_data->data()->Get(0) ;
            break ;
          }
          case Tables::DataTypes_FBU_SDT_FLOAT_FBU : {
            auto float_col_data = static_cast< const Tables::SDT_FLOAT_FBU* >( curr_rec_data->Get(j) ) ;
            std::cout << float_col_data->data()->Get(0) ;
            break ;
          }
          case Tables::DataTypes_FBU_SDT_STRING_FBU : {
            auto string_col_data = static_cast< const Tables::SDT_STRING_FBU* >( curr_rec_data->Get(j) ) ;
            std::cout << string_col_data->data()->Get(0)->str() ;
            break ;
          }
          default :
            std::cout << "execute_query: unrecognized row_data_type "
                      << (unsigned)curr_rec_data_type->Get(j) << std::endl ;
            exit(1) ;
        } //switch

        if( j < curr_rec_data->Length()-1 )
          std::cout << "," ;

      } //for loop
      std::cout << std::endl ;
    } //for loop
  } // Relation_Rows

  // process one Root>Cols flatbuffer
  else if( format == Tables::Relation_FBU_Cols_FBU ) {
    if( print_verbose )
      std::cout << "else if data_type == Tables::Relation_FBU_Cols_FBU" << std::endl ;

    auto cols = static_cast< const Tables::Cols_FBU* >( data ) ;
    auto cols_data = cols->data() ;

    // collect data for stdout printing
    std::vector< std::vector< std::string > > out_data ;
    for( unsigned int i = 0; i < cols_data->Length(); i++ ) {
      std::vector< std::string > empty_vect ;
      out_data.push_back( empty_vect ) ;
    }

    std::vector< uint64_t > rids ;
    for( unsigned int i = 0; i < cols_data->Length(); i++ ) {

      auto col = static_cast< const Tables::Col_FBU* >( cols_data->Get(i) ) ;
      auto col_rids      = col->RIDs() ;
      auto col_data      = col->data() ;
      auto col_data_type = col->data_type() ;

      if( i == 0 ) {
        for( unsigned int k = 0; k < col_rids->Length(); k++ )
          rids.push_back( col_rids->Get(k) ) ;
      }

      for( unsigned int j = 0; j < nrows; j++ ) {
        switch( (unsigned)col_data_type ) {
          case Tables::DataTypes_FBU_SDT_UINT64_FBU : {
            auto int_col_data = static_cast< const Tables::SDT_UINT64_FBU* >( col_data ) ;
            out_data[i].push_back( std::to_string( int_col_data->data()->Get(j) ) ) ;
            break ;
          }
          case Tables::DataTypes_FBU_SDT_FLOAT_FBU : {
            auto float_col_data = static_cast< const Tables::SDT_FLOAT_FBU* >( col_data ) ;
            out_data[i].push_back( std::to_string( float_col_data->data()->Get(j) ) ) ;
            break ;
          }
          case Tables::DataTypes_FBU_SDT_STRING_FBU : {
            auto string_col_data = static_cast< const Tables::SDT_STRING_FBU* >( col_data ) ;
            out_data[i].push_back( string_col_data->data()->Get(j)->str() ) ;
            break ;
          }
          default :
            std::cout << "unrecognized data_type " << (unsigned)col_data_type << std::endl ;
            exit(1) ;
        } //switch
      } //for
    } //for

    for( unsigned int i = 0; i < nrows; i++ ) {
      std::string this_row = "" ;
      this_row = this_row + std::to_string( rids[i] ) + ":\t" ;
      for( unsigned int j = 0; j < out_data.size(); j++ ) {
        this_row = this_row + out_data[j][i] ;
        if( j < ( out_data.size()-1 ) )
          this_row = this_row + "," ;
      }
      std::cout << this_row << std::endl ;
    } //for

  } // Relation_Cols

  else {
    std::cout << "unrecognized format '" << format << "'" << std::endl ;
    exit(1) ;
  }

  return 0 ;
} //printFlatbufFBUAsCSV

