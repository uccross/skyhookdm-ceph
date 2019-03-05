/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include <fstream>
#include "vop_engine.h"

void execute_query ( vop_query vq, librados::bufferlist relation_blist ) {

  std::cout << "execute_query with params:" << std::endl ;
  std::cout << "  vq       : " << vq.toString() << std::endl ;
  std::cout << "  relation : " << std::endl ;

  std::vector< dataset_transform > decoded_relation_blist ; // vector of dataset_transform types
  librados::bufferlist::iterator decoded_relation_biter( &relation_blist ) ;
  ::decode( decoded_relation_blist, decoded_relation_biter ) ;

  for( unsigned int i = 0; i < decoded_relation_blist.size(); i++ ) {
    std::cout << decoded_relation_blist[i].toString()
              << " > schema " << decoded_relation_blist[i].schema 
              << " > arity " << decoded_relation_blist[i].schema.size() << std::endl ;
  }

  // extract data
  // for each dataset, for each attribute, if the attribute is projected (>-1),
  // collect the value in a row. 
  std::vector< std::vector< std::string > > vop_query_results ;
  int num_datasets = decoded_relation_blist.size() ;
  for( int i = 0; i < num_datasets; i++ ) {
    std::vector< std::string > row ;
    auto curr_dataset_data   = decoded_relation_blist[i].data ;
    auto curr_dataset_schema = decoded_relation_blist[i].schema ;
    int dataset_arity        = curr_dataset_schema.size() ;
    int projection_arity     = vq.attribute_vect.size() ;
    //std::cout << "curr_dataset_data   : " << curr_dataset_data << std::endl ;
    //std::cout << "curr_dataset_schema : " << curr_dataset_schema << std::endl ;
    for( int j = 0; j < projection_arity; j++ ) {
      auto curr_projection_att = vq.attribute_vect[j] ;
      //std::cout << "curr_projection_att : " << curr_projection_att << std::endl ;
      for( int k = 0; k < dataset_arity; k++ ) {
        auto curr_dataset_att = curr_dataset_schema[k] ;
        //std::cout << "curr_dataset_att : " << curr_dataset_att << std::endl ;
        if( curr_projection_att == curr_dataset_att ) {
          //std::cout << "true" << std::endl ;
          row.push_back( curr_dataset_data[k].first ) ;
        }
        else {
          //std::cout << "false" << std::endl ;
        }
      }
    }
    vop_query_results.push_back( row ) ;
  }

  std::cout << vop_query_results << std::endl ;
}
