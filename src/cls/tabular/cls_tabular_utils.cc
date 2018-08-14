/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include "cls_tabular_utils.h"

#include <errno.h>
#include <string>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <time.h>
#include "include/types.h"

#include "objclass/objclass.h"

#include "cls_tabular.h"
#include "flatbuffers/flexbuffers.h"
#include "skyhookv1_generated.h"

namespace Tables {

int extractSchema(vector<struct col_info> &schema, string& schema_string) {

    vector<std::string> elems;
    boost::split(elems, schema_string, boost::is_any_of("\n"),
            boost::token_compress_on);

    // assume schema string contains at least one col's info
    if (elems.size() < 1)
        return TablesErrCodes::EmptySchema;

    for (vector<string>::iterator it = elems.begin(); it != elems.end(); ++it) {

        vector<std::string> col_data;  // each col string describes one col structure
        std::string col_info_string = *it;
        boost::trim(col_info_string);

        // ignore empty strings after trimming, due to above boost split.
        if (col_info_string.length() < 4)
            continue;

        boost::split(col_data, col_info_string, boost::is_any_of(" "),
                boost::token_compress_on);

        // expected num of items in our col_info struct
        if (col_data.size() != 4)
            return TablesErrCodes::BadColInfoFormat;

        const struct col_info ci(col_data[0], col_data[1], col_data[2],
                col_data[3]);
        schema.push_back(ci);
    }
    return 0;
}

void printSkyRootHeader(sky_root_header *root){
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

void printSkyRows(const char* fb, size_t fb_size,
        vector<struct col_info> &schema) {

    cout << "\nTODO:extract and print rows here." << endl;

    // print col header
    for (vector<struct col_info>::iterator it = schema.begin(); it != schema.end(); ++it) {
        cout << " | " << (*it).name;
    }
    cout << endl;
}

sky_root_header* getSkyRootHeader(const char* fb, size_t fb_size)
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

} // end namespace Tables
