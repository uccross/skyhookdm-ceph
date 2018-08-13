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

    // assume schema has at least one col
    if (elems.size() < 1)
        return 1;

    for (vector<string>::iterator it = elems.begin(); it != elems.end(); ++it) {

        vector<std::string> s;
        boost::split(s, *it, boost::is_any_of(" "), boost::token_compress_on);

        // expected num of items in our col_info struct
        if (s.size() != 4)
            return 1;

        struct col_info c = col_info(s[0], s[1], s[2], s[3]);
        schema.push_back(c);
    }
    return 0;
}

void printSkyRootHeader(sky_root_header *root)
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

void printSkyRows(sky_root_header *root)
{
    /* TODO: use our flatbuf row reader from
     * src/progly/flatbuffers/read_write_flatbuffs
     */
    cout << "\nTODO:extract and print rows here." << endl;
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
