#include <iostream>
#include "cls_dm.h"

// ================================================= //
//                    GET INDEX                      //
// ================================================= //

int get_index( Dataset ds, std::string pattern ) {

  int inx = -1 ;

  switch( ds.type_code ) {
    case 0 :
      inx = get_index( ds.ds_str, pattern ) ;
      break ;
    case 1 :
      inx = get_index( ds.ds_vect_str, pattern ) ;
      break ;
    case 3 :
      //inx = get_index( ds.ds_vvop, pattern ) ;
      break ;
  }

  return inx ;
}

int get_index( Dataset ds, uint64_t pattern ) {
  return get_index( ds.ds_vect_uint64, pattern ) ;
}

int get_index( dataset_str data_str, std::string pattern ) {
  return data_str.data.find( pattern ) ;
}

int get_index( dataset_vect_str data_vect_str, std::string pattern ) {
  int inx = -1 ;
  for( int i = 0; i < data_vect_str.getLength(); i++ ) {
    if( data_vect_str.data[i] == pattern ) {
      inx = i ;
      break ;
    }
  }
  return inx ;
}

int get_index( dataset_vect_uint64 data_vect_uint64, uint64_t pattern ) {
  int inx = -1 ;
  for( int i = 0; i < data_vect_uint64.getLength(); i++ ) {
    if( data_vect_uint64.data[i] == pattern ) {
      inx = i ;
      break ;
    }
  }
  return inx ;
}

//int get_index( vvop data_vvop ) {
//}

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

  std::cout << "left_index:" << std::endl ;
  std::cout << left_index << std::endl ;
  std::cout << "right_index:" << std::endl ;
  std::cout << right_index << std::endl ;

  std::vector< std::string > subvect ;
  for( int i = left_index; i < right_index; i++ ) {
    subvect.push_back( data_vect_str.data[i] ) ;
  }

  ds_range.ds_vect_str.data = subvect ;

  return ds_range ;
}

Dataset get_range( dataset_vect_uint64 data_vect_uint64, 
                   int left_index, 
                   int right_index ) {
  Dataset ds_range ;
  ds_range.type_code = 2 ;

  std::vector< uint64_t > subvect ;
  for( int i = left_index; i < right_index; i++ ) {
    subvect.push_back( data_vect_uint64.data[i] ) ;
  }

  ds_range.ds_vect_uint64.data = subvect ;

  return ds_range ;
}

Dataset get_range( vvop data_vvop, 
                   int left_index, 
                   int right_index ) {
  Dataset ds_range ;
  ds_range.type_code = 3 ;

  std::vector< std::vector< std::pair< std::string, int > > > subvect ;
  for( int i = left_index; i < right_index; i++ ) {
    subvect.push_back( data_vvop.data[i] ) ;
  }

  ds_range.ds_vvop.data = subvect ;

  return ds_range ;
}


// ================================================= //
//                    GET REVERSE                    //
// ================================================= //

Dataset get_reverse( Dataset ds ) {

  Dataset ds_reverse ;

  switch( ds.type_code ) {
    case 0 :
      ds_reverse = get_reverse( ds.ds_str ) ;
      break ;
    case 1 :
      ds_reverse = get_reverse( ds.ds_vect_str ) ;
      break ;
    case 2 :
      ds_reverse = get_reverse( ds.ds_vect_uint64 ) ;
      break ;
    case 3 :
      ds_reverse = get_reverse( ds.ds_vvop ) ;
      break ;
  }

  return ds_reverse ;
}

Dataset get_reverse( dataset_str data_str ) {
  Dataset ds_reverse ;
  ds_reverse.type_code = 0 ;
  for( int i = data_str.data.size() - 1 ; i >=0 ; i-- ) {
    ds_reverse.ds_str.data.push_back( data_str.data[i] ) ;
  }
  return ds_reverse ;
}

Dataset get_reverse( dataset_vect_str data_vect_str ) {
  Dataset ds_reverse ;
  ds_reverse.type_code = 1 ;
  for( int i = data_vect_str.data.size() - 1 ; i >=0 ; i-- ) {
    ds_reverse.ds_vect_str.data.push_back( data_vect_str.data[i] ) ;
  }
  return ds_reverse ;
}

Dataset get_reverse( dataset_vect_uint64 data_vect_uint64 ) {
  Dataset ds_reverse ;
  ds_reverse.type_code = 2 ;
  for( int i = data_vect_uint64.data.size() - 1 ; i >=0 ; i-- ) {
    ds_reverse.ds_vect_uint64.data.push_back( data_vect_uint64.data[i] ) ;
  }
  return ds_reverse ;
}

Dataset get_reverse( vvop data_vvop ) {
  Dataset ds_reverse ;
  ds_reverse.type_code = 3 ;
  for( int i = data_vvop.data.size() - 1 ; i >=0 ; i-- ) {
    ds_reverse.ds_vvop.data.push_back( data_vvop.data[i] ) ;
  }
  return ds_reverse ;
}

// ================================================= //
//                    GET SORT                       //
// ================================================= //

Dataset get_sort( Dataset ds ) {

  Dataset ds_sort ;

  switch( ds.type_code ) {
    case 0 :
      ds_sort = get_sort( ds.ds_str ) ;
      break ;
    case 1 :
      ds_sort = get_sort( ds.ds_vect_str ) ;
      break ;
    case 2 :
      ds_sort = get_sort( ds.ds_vect_uint64 ) ;
      break ;
    case 3 :
      ds_sort = get_sort( ds.ds_vvop ) ;
      break ;
  }

  return ds_sort ;
}

Dataset get_sort( dataset_str data_str ) {
  Dataset ds_sort ;
  ds_sort.type_code = 0 ;
  std::sort( data_str.data.begin(), data_str.data.end() ) ;
  ds_sort.ds_str = data_str ;
  return ds_sort ;
}

Dataset get_sort( dataset_vect_str data_vect_str ) {
  Dataset ds_sort ;
  ds_sort.type_code = 1 ;
  std::sort( data_vect_str.data.begin(), data_vect_str.data.end() ) ;
  ds_sort.ds_vect_str = data_vect_str ;
  return ds_sort ;
}

Dataset get_sort( dataset_vect_uint64 data_vect_uint64 ) {
  Dataset ds_sort ;
  ds_sort.type_code = 2 ;
  std::sort( data_vect_uint64.data.begin(), data_vect_uint64.data.end() ) ;
  ds_sort.ds_vect_uint64 = data_vect_uint64 ;
  return ds_sort ;
}

Dataset get_sort( vvop data_vvop ) {
  Dataset ds_sort ;
  ds_sort.type_code = 3 ;
  std::sort( data_vvop.data.begin(), data_vvop.data.end() ) ;
  ds_sort.ds_vvop = data_vvop ;
  return ds_sort ;
}

// ================================================= //
//                    GET TRANSPOSE                  //
// ================================================= //

Dataset get_transpose( Dataset ds ) {

  Dataset ds_transpose ;

  switch( ds.type_code ) {
    case 3 :
      ds_transpose = get_transpose( ds.ds_vvop ) ;
      break ;
  }

  return ds_transpose ;
}

Dataset get_transpose( vvop data_vvop ) {
  Dataset ds_transpose ;
  ds_transpose.type_code = 3 ;
  // peel out the ith element from every row and insert
  // into the corresponding ith col

  int arity = data_vvop.data[0].size() ;
  //std::cout << "arity = " << arity << std::endl ;
  for( int i = 0; i < arity; i++ ) {
    std::vector< std::pair< std::string, int > > row ;
    for( unsigned int j = 0; j < data_vvop.getLength(); j++ ) { 
      //std::cout << data_vvop.data[j][i] << std::endl ;
      row.push_back( data_vvop.data[j][i] ) ;
    }
    //std::cout << "pushback row : " << row << std::endl ;
    ds_transpose.ds_vvop.data.push_back( row ) ;
  }
  return ds_transpose ;
}
