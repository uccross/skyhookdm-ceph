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

// schema string expects the format cls_tabular_utils.h lineitem_test_schema
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

        std::string name = col_data[3];
        boost::trim(name);
        const struct col_info ci(col_data[0], col_data[1], col_data[2], name);
        schema.push_back(ci);
    }
    return 0;
}

void printSkyRootHeader(sky_root_header &r) {
    cout << "\n[ROOT HEADER]"<< endl;
    cout << "skyhook version: "<< r.skyhook_version << endl;
    cout << "schema version: "<< r.schema_version << endl;
    cout << "table name: "<< r.table_name << endl;
    cout << "schema: "<< r.schema << endl;
    cout<< "delete vector: [";
    for (int i=0; i< (int)r.delete_vec.size(); i++) {
        cout << (int)r.delete_vec[i];
        if (i != (int)r.delete_vec.size()-1)
            cout<<", ";
    }
    cout << "]" << endl;
    cout << "nrows: " << r.nrows << endl;
    cout << endl;
}

void printSkyRowHeader(sky_row_header &r) {

    cout << "\n[ROW HEADER]" << endl;
    cout << "RID: "<< r.RID << endl;

    std::string bitstring = "";
    int64_t val = 0;
    for(int j = 0; j < (int)r.nullbits.size(); j++) {
        val = r.nullbits.at(j);
        for (uint64_t k=0; k < 8 * sizeof(r.nullbits.at(j)); k++) {
            uint64_t mask =  1 << k;
            uint64_t bit = 0;
            ((val&mask)>0) ? bit=1 : bit=0;
            bitstring.append(std::to_string(bit));
        }
        cout << "nullbits ["<< j << "]: val=" << val << ": bits=" << bitstring;
        cout << endl;
        bitstring.clear();
    }
    cout << endl;
}

// parent print function for skyhook flatbuffer data layout
void printSkyFb(const char* fb, size_t fb_size,
        vector<struct col_info> &schema) {

    // get root table ptr
    sky_root_header skyroot = Tables::getSkyRootHeader(fb, fb_size);
    printSkyRootHeader(skyroot);

    // get row table ptrs
	const flatbuffers::Vector<flatbuffers::Offset<Row>>* recs = \
            GetTable(fb)->rows();

    // print row metadata
    for (int i = 0; i < skyroot.nrows; i++) {
        sky_row_header skyrow = getSkyRowHeader(recs->Get(i));
        printSkyRowHeader(skyrow);
    }

    // print col metadata (only names for now).
    for (vector<struct col_info>::iterator it = schema.begin();
            it != schema.end(); ++it) {
        cout << " | " << (*it).name;
    }
    cout << endl;
    cout << "\nTODO: print row data here based on schema." << endl;
}

sky_root_header getSkyRootHeader(const char *fb, size_t fb_size) {

    const Table* root = GetTable(fb);

    return sky_root_header (
            root->skyhook_version(),
            root->schema_version(),
            root->table_name()->str(),
            root->schema()->str(),
            delete_vector (root->delete_vector()->begin(),
                           root->delete_vector()->end()),
            rows_vector (root->rows()->begin(),  root->rows()->end()),
            root->nrows());
}

sky_row_header getSkyRowHeader(const Tables::Row* rec) {

    return sky_row_header(
            rec->RID(),
            nullbits_vector (rec->nullbits()->begin(), rec->nullbits()->end()),
            rec->data_flexbuffer_root());
}

} // end namespace Tables
