#include <iostream>
#include "cls_dm.h"

// ================================================= //
//                    GET MIDPOINT                   //
// ================================================= //

int get_midpoint( Dataset ds ) {

  int mid = -1 ;

  switch( ds.type_code ) {
    case 0 :
      mid = get_midpoint( ds.ds_str ) ;
      break ;
    case 1 :
      mid = get_midpoint( ds.ds_vect_str ) ;
      break ;
    case 2 :
      mid = get_midpoint( ds.ds_vect_uint64 ) ;
      break ;
    case 3 :
      mid = get_midpoint( ds.ds_vvop ) ;
      break ;
  }

  return mid ;
}

int get_midpoint( dataset_str ds_str ) {
  int mid = ds_str.data.size() / 2 ;
  return mid ;
}

int get_midpoint( dataset_vect_str ds_vect_str ) {
  int mid = ds_vect_str.data.size() / 2 ;
  return mid ;
}

int get_midpoint( dataset_vect_uint64 ds_vect_uint64 ) {
  int mid = ds_vect_uint64.data.size() / 2 ;
  return mid ;
}

int get_midpoint( vvop ds_vvop ) {
  int mid = ds_vvop.data.size() / 2 ;
  return mid ;
}

// ================================================= //
//                    GET RANGE                      //
// ================================================= //

Dataset get_range( Dataset ds, 
                   int left_index, 
                   int right_index ) {

  Dataset ds_range ;

  switch( ds.type_code ) {
    case 0 :
      ds_range = get_range( ds.ds_str, left_index, right_index ) ;
      break ;
    case 1 :
      ds_range = get_range( ds.ds_vect_str, left_index, right_index ) ;
      break ;
    case 2 :
      ds_range = get_range( ds.ds_vect_uint64, left_index, right_index ) ;
      break ;
    case 3 :
      ds_range = get_range( ds.ds_vvop, left_index, right_index ) ;
      break ;
  }

  return ds_range ;
}

Dataset get_range( dataset_str data_str, 
                   int left_index, 
                   int right_index ) {
  Dataset ds_range ;
  ds_range.type_code = 0 ;
  ds_range.ds_str.data = data_str.data.substr( left_index, right_index-left_index ) ;
  return ds_range ;
}

Dataset get_range( dataset_vect_str data_vect_str, 
                   int left_index, 
                   int right_index  ) {
  Dataset ds_range ;
  ds_range.type_code = 1 ;

  std::vector< std::string >::const_iterator first = data_vect_str.data.begin() + 
                                                     left_index ;
  std::vector< std::string >::const_iterator last  = data_vect_str.data.begin() + 
                                                     ( right_index - left_index ) ;
  std::vector< std::string > subvect( first, last ) ;

  ds_range.ds_vect_str.data = subvect ;

  return ds_range ;
}

Dataset get_range( dataset_vect_uint64 data_vect_uint64, 
                   int left_index, 
                   int right_index ) {
  Dataset ds_range ;
  ds_range.type_code = 2 ;

  std::vector< uint64_t >::const_iterator first = data_vect_uint64.data.begin() + 
                                                  left_index ;
  std::vector< uint64_t >::const_iterator last  = data_vect_uint64.data.begin() + 
                                                  ( right_index - left_index ) ;
  std::vector< uint64_t > subvect( first, last ) ;

  ds_range.ds_vect_uint64.data = subvect ;

  return ds_range ;
}

Dataset get_range( vvop data_vvop, 
                   int left_index, 
                   int right_index ) {
  Dataset ds_range ;
  ds_range.type_code = 3 ;

  std::vector< std::vector<std::pair<std::string,int>> >::const_iterator first = data_vvop.data.begin() + 
                                                  left_index ;
  std::vector< std::vector<std::pair<std::string,int>> >::const_iterator last  = data_vvop.data.begin() + 
                                                  ( right_index - left_index ) ;
  std::vector< std::vector<std::pair<std::string,int>> > subvect( first, last ) ;

  ds_range.ds_vvop.data = subvect ;

  return ds_range ;
}
