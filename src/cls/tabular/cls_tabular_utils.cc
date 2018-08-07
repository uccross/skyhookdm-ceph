/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include <errno.h>
#include <string>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <time.h>
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_tabular.h"
#include "cls_tabular_utils.h"
#include "flatbuffers/flexbuffers.h"
#include "skyhookv1_generated.h"

void dummyfunc(int val) 
{
    std::cout << std::endl;
}

/* 
 * Extracts the schema format (cols/types) from the base schema string. 
 * Used for both the Table's current schema and the query's return schema
 * TODO: remove from top level fb and store in omap to better support 
 *             schema evolution.
 */ 
int Tables::getSchemaFormat(std::string schema_string, vector<col_info>& cols) 
{

	// Parse Schema string for Composite Key
    vector<std::string> lines = delimStringSplit(schema_string, '\n');
    if(lines.size() < 1) {
      return -1;
    }
    
	// Extract compositeKey (this is the partitioning key)
    // currently storate as the first line in a schema string.
	vector<string> compositeKey;
	compositeKey = delimStringSplit(lines.at(0),' ');

	// Extract col nums/Data Types
	for (unsigned int i = 1; i < lines.size(); i++) {
        vector<string> vals = delimStringSplit(lines.at(i), ' ');
        col_info c = {atoi(vals[0].c_str()), atoi(vals[1].c_str()), vals[2]};
        cols.push_back(c);
	}
	
	return 0;
}

inline std::vector<std::string> Tables::delimStringSplit(const std::string &s, char delim) 
{
	std::istringstream ss(s);
	std::string item;
	std::vector<std::string> tokens;
	while(getline(ss, item, delim)) {
		tokens.push_back(item);
	}
	return tokens;
}

void Tables::printSkyRootHeader(sky_root_header *root)
{
    
	cout<<"\n[ROOT HEADER]"<<endl;
	cout<<"skyhook version: "<<root->skyhook_version<<endl;
	cout<<"schema version: "<<root->schema_version<<endl;
	cout<<"table name: "<<root->table_name<<endl;
	cout<<"schema: "<<root->schema<<endl;
	cout<<"delete vector: [";
	for(int i=0;i< (int)root->delete_vec.size();i++) {
		cout<<(int)root->delete_vec[i];
		if(i != (int)root->delete_vec.size()-1 )
			cout<<", ";
	}
	cout<<"]"<<endl;
	cout<<"row offset: "<<root->rows_offset<<endl;
	cout<<"nrows: "<<root->nrows<<endl;
	cout<<endl;
}

void Tables::printSkyRows(sky_root_header *root)
{
    /* TODO: use our flatbuf row reader from 
     * src/progly/flatbuffers/read_write_flatbuffs
     */
    std::cout << std::endl << "TODO:extract and print rows here." << std::endl;
}

Tables::sky_root_header* Tables::getSkyRootHeader(const char* fb, size_t fb_size)
{
    auto table = GetTable(fb);
    sky_root_header *r = new sky_root_header();
	r->skyhook_version = table->skyhook_version();
	r->schema_version = table->schema_version();
	r->table_name = table->table_name()->str();
	r->schema = table->schema()->str();
	auto root_delete_vec = table->delete_vector();
	for(int i=0;i<(int)root_delete_vec->size();i++)
		r->delete_vec.push_back(root_delete_vec->Get(i));
	r->nrows = table->nrows();

    return r;
}
