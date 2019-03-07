#include <iostream>
#include "cls_dm.h"

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

std::pair<Dataset,Dataset> get_range( Dataset ds, 
               int left_index, 
               int right_index ) {

  Dataset left ;
  Dataset right ;

  switch( ds.type_code ) {
    case 0 :
      std::cout << "type 0" << std::endl ;
      break ;
    case 1 :
      std::cout << "type 1" << std::endl ;
      break ;
    case 2 :
      std::cout << "type 2" << std::endl ;
      break ;
    case 3 :
      std::cout << "type 3" << std::endl ;
      break ;
  }

  return std::make_pair( left, right ) ;

}

std::pair< Dataset, Dataset > get_range( dataset_str ds_str, 
                                         int left_index, 
                                         int right_index ) {
  Dataset left ;
  Dataset right ;
  return std::make_pair( left, right ) ;
}

std::pair< Dataset, Dataset > get_range( dataset_vect_str ds_vect_str, 
                                         int left_index, 
                                         int right_index  ) {
  Dataset left ;
  Dataset right ;
  return std::make_pair( left, right ) ;
}

std::pair< Dataset, Dataset > get_range( dataset_vect_uint64 ds_vect_uint64, 
                                         int left_index, 
                                         int right_index ) {
  Dataset left ;
  Dataset right ;
  return std::make_pair( left, right ) ;
}

std::pair< Dataset, Dataset > get_range( vvop ds_vvop, 
                                         int left_index, 
                                         int right_index ) {
  Dataset left ;
  Dataset right ;
  return std::make_pair( left, right ) ;
}
