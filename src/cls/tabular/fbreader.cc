
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
#include "skyroot_generated.h"

namespace po = boost::program_options ;

void do_read( bool, std::string, std::string ) ;
std::string printVect( std::vector< std::string > ) ;

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

// =========== //
//  printVect  //
// =========== //
std::string printVect( std::vector< std::string > vec ) {
  std::ostringstream oss ;
  std::copy( vec.begin(), 
             vec.end()-1, 
             std::ostream_iterator< std::string >( oss, "," ) ) ;
  oss << vec.back();
  return oss.str() ;
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

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( pool.c_str(), ioctx ) ;

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;

  if( debug ) {
    std::cout << "num_bytes_read : " << num_bytes_read << std::endl ; 
  }

  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // ================================================================================ //
  // display data

  while( it_wrapped.get_remaining() > 0 ) {

    if( debug ) {
      std::cout << "it_wrapped.get_remaining() = " << it_wrapped.get_remaining() << std::endl ;
    }

    // grab the Root
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* fb = bl.c_str() ;

    auto root      = Tables::GetRoot( fb ) ;
    auto data_type = root->relationData_type() ;

    if( debug ) {
      std::cout << "data_type : " << data_type << std::endl ;
    }

    // process one Root>Rows flatbuffer
    if( data_type == Tables::Relation_Rows ) {
      if( debug ) {
        std::cout << "if data_type == Tables::Relation_Rows" << std::endl ;
      }

      auto schema    = root->schema() ;
      if( debug ) {
        std::cout << "schema : " << std::endl ;
        for( unsigned int i = 0; i < schema->Length(); i++ )
          std::cout << (unsigned)schema->Get(i) << std::endl ;
      }
 
      auto rows = static_cast< const Tables::Rows* >( root->relationData() ) ;
      auto table_name_read = rows->table_name() ;

      auto schema_read     = rows->schema() ;
      auto nrows_read      = rows->nrows() ;
      auto ncols_read      = rows->ncols() ;
      auto rids_read       = rows->RIDs() ;
      auto rows_data       = rows->data() ; // [ Record ]

      if( debug ) {
        std::cout << "table_name_read->str() : " << table_name_read->str() << std::endl ;
        std::cout << "schema_read->Length() : " << schema_read->Length() << std::endl ;
        std::cout << "nrows_read     : " << nrows_read     << std::endl ;
        std::cout << "ncols_read     : " << ncols_read     << std::endl ;
      }

      // print schema to stdout
      if( debug ) {
        std::cout << "RID\t" ;
        for( unsigned int i = 0; i < ncols_read; i++ ) {
          std::cout << schema_read->Get(i)->str() << "\t" ;
        }
        std::cout << std::endl ;
      }

      //auto int_data = static_cast< const Tables::IntData* >( rows_data->Get(0) ) ;
      //std::cout << int_data->data()->Get(0) << std::endl ;
      // print data to stdout
      for( unsigned int i = 0; i < rows_data->Length(); i++ ) {
        if( debug ) {
          std::cout << rids_read->Get(i) << ":\t" ;
        }

        auto curr_rec           = rows_data->Get(i) ;
        auto curr_rec_data      = curr_rec->data() ;
        auto curr_rec_data_type = curr_rec->data_type() ;

        //std::cout << "curr_rec_data->Length() = " << curr_rec_data->Length() << std::endl ;

        for( unsigned int j = 0; j < curr_rec_data->Length(); j++ ) {
          // column of ints
          if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_IntData ) {
            //std::cout << "int" << "\t" ;
            auto int_col_data = static_cast< const Tables::IntData* >( curr_rec_data->Get(j) ) ;
            std::cout << int_col_data->data()->Get(0) ;
          }
          // column of floats
          else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_FloatData ) {
            //std::cout << "float" << "\t" ;
            auto float_col_data = static_cast< const Tables::FloatData* >( curr_rec_data->Get(j) ) ;
            std::cout << float_col_data->data()->Get(0) ;
          }
          // column of strings
          else if( (unsigned)curr_rec_data_type->Get(j) == Tables::Data_StringData ) {
            //std::cout << "str" << "\t" ;
            auto string_col_data = static_cast< const Tables::StringData* >( curr_rec_data->Get(j) ) ;
            std::cout << string_col_data->data()->Get(0)->str() ;
          }
          else {
            std::cout << "execute_query: unrecognized row_data_type " 
                      << (unsigned)curr_rec_data_type->Get(j) << std::endl ; 
          }

          if( j < curr_rec_data->Length()-1 )
            std::cout << "," ;
        }
        std::cout << std::endl ;
      }
    } // Relation_Rows 

    // process one Root>Col flatbuffer
    else if( data_type == Tables::Relation_Col ) {
      if( debug ) {
        std::cout << "else if data_type == Tables::Relation_Col" << std::endl ;
      }

      auto col = static_cast< const Tables::Col* >( root->relationData() ) ;
      auto col_name_read  = col->col_name() ;
      auto col_index_read = col->col_index() ;
      auto nrows_read     = col->nrows() ;
      auto rids_read      = col->RIDs() ;
      auto col_data_type  = col->data_type() ;
      auto col_data       = col->data() ;

      if( debug ) {
        std::cout << "col_name_read->str() : " << col_name_read->str() << std::endl ;
        std::cout << "col_index_read       : " << (unsigned)col_index_read << std::endl ;
        std::cout << "nrows_read           : " << (unsigned)nrows_read     << std::endl ;
        std::cout << "col_data_type        : " << col_data_type     << std::endl ;
      }

      // print data to stdout
      for( unsigned int i = 0; i < nrows_read; i++ ) {
        if( debug ) {
          std::cout << rids_read->Get(i) << ":\t" ;
        }
        // column of ints
        if( (unsigned)col_data_type == Tables::Data_IntData ) {
          auto int_col_data = static_cast< const Tables::IntData* >( col_data ) ;
          std::cout << int_col_data->data()->Get(i) ;
        }
        // column of floats
        else if( (unsigned)col_data_type == Tables::Data_FloatData ) {
          auto float_col_data = static_cast< const Tables::FloatData* >( col_data ) ;
          std::cout << float_col_data->data()->Get(i) ;
        }
        // column of strings
        else if( (unsigned)col_data_type == Tables::Data_StringData ) {
          auto string_col_data = static_cast< const Tables::StringData* >( col_data ) ;
          std::cout << string_col_data->data()->Get(i)->str() ;
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

    if( debug ) {
      std::cout << "loop while" << std::endl ;
    }

  } // while

  ioctx.close() ;

} // do_read
