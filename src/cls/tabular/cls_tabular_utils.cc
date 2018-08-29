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

int processSkyFb(
    flatbuffers::FlatBufferBuilder &flatbldr,
    schema_vec &schema_in,
    schema_vec &schema_out,
    const char *fb,
    const size_t fb_size,
    std::string &errmsg)
{
    int errcode = 0;
    Tables::delete_vector dead_rows;
    std::vector<flatbuffers::Offset<Tables::Row>> offs;
    Tables::sky_root_header skyroot = Tables::getSkyRootHeader(fb, fb_size);

    // identify the max col idx, to prevent flexbuf vector oob error
    int col_idx_max, col_idx_min;
    col_idx_max = col_idx_min = -1;
    for (auto it=schema_in.begin(); it!=schema_in.end(); ++it) {
        if ((*it).idx > col_idx_max)
            col_idx_max = (*it).idx;
    }

    // build the flexbuf for each row's data
    for (uint32_t i = 0; i < skyroot.nrows && !errcode; i++) {

        if (skyroot.delete_vec.at(i) == 1) continue;  // skip dead rows.

        sky_row_header skyrow = getSkyRowHeader(skyroot.offs->Get(i));
        auto row = skyrow.data.AsVector();
        flexbuffers::Builder *flexbldr = new flexbuffers::Builder();
        flexbldr->Vector([&]() {

            // iter over the output schema, locating it within the input schema.
            for (auto it=schema_out.begin(); it!=schema_out.end() && !errcode; ++it) {

                Tables::col_info col = *it;
                if (col.idx < col_idx_min || col.idx > col_idx_max) {
                    errcode = TablesErrCodes::RequestedColIndexOOB;
                    errmsg.append("ERROR processSkyFb(): table=" +
                            skyroot.table_name + "; rid=" +
                            std::to_string(skyrow.RID) + " col.idx=" +
                            std::to_string(col.idx) + " OOB.");

                } else if (col.idx < 0) {

                    // A negative col id # means not in original schema
                    // Do some processing (SUM, COUNT, etc)

                } else {

                    switch(col.type) {
                    case Tables::SkyTypeInt: {
                        int value = row[col.idx].AsInt32();
                        flexbldr->Int(value);
                        break;
                    }
                    case Tables::SkyTypeDouble: {
                        double value = row[col.idx].AsDouble();
                        flexbldr->Double(value);
                        break;
                    }
                    case Tables::SkyTypeChar: {
                        int8_t value = row[col.idx].AsInt8();
                        flexbldr->Int(value);
                        break;
                    }
                    case Tables::SkyTypeDate: {
                        string s = row[col.idx].AsString().str();
                        flexbldr->String(s);
                        break;
                    }
                    case Tables::SkyTypeString: {
                        string s = row[col.idx].AsString().str();
                        flexbldr->String(s);
                        break;
                    }
                    default: {
                        errcode = TablesErrCodes::UnsupportedSkyDataType;
                        errmsg.append("ERROR processSkyFb(): table=" +
                                skyroot.table_name + "; rid=" +
                                std::to_string(skyrow.RID) + " col.type=" +
                                std::to_string(col.type) +
                                " UnsupportedSkyDataType.");
                        break;
                    }}
                }
            }
        });

        // finalize the DATA flexbuf
        flexbldr->Finish();

        // build the return ROW flatbuf that contains our row data flexbuf
        auto row_data = flatbldr.CreateVector(flexbldr->GetBuffer());
        delete flexbldr;
        auto nullbits = flatbldr.CreateVector(skyrow.nullbits);  // TODO: update nullbits
        flatbuffers::Offset<Tables::Row> row_off = \
                Tables::CreateRow(flatbldr, skyrow.RID, nullbits, row_data);

        // Continue building the ROOT flatbuf's dead vector and rowOffsets vector
        dead_rows.push_back(0);
        offs.push_back(row_off);
    }

    // build the return ROOT flatbuf
    std::string schema_str;
    for (auto it = schema_out.begin(); it != schema_out.end(); ++it) {
        schema_str.append((*it).toString() + "\n");
    }
    auto table_name = flatbldr.CreateString(skyroot.table_name);
    auto ret_schema = flatbldr.CreateString(schema_str);
    auto delete_v = flatbldr.CreateVector(dead_rows);
    auto rows_v = flatbldr.CreateVector(offs);
    auto tableOffset = CreateTable(flatbldr, skyroot.skyhook_version,
        skyroot.schema_version, table_name, ret_schema,
        delete_v, rows_v, offs.size());

    // NOTE: fb may be incomplete, but we need to finish it else fb assert fail
    // hence we always return a valid fb and must catch ret error code upstream
    flatbldr.Finish(tableOffset);

    return errcode;
}

// obtain the schema of the table or the query
void getSchema(schema_vec &schema, std::string schema_str) {

    // TODO: don't use a string, get from omap or the obj directly.
    int ret = extractSchemaFromString(schema, schema_str);

    assert(ret!=TablesErrCodes::EmptySchema);
    assert(ret!=TablesErrCodes::BadColInfoFormat);
}

// schema string expects the format in cls_tabular_utils.h lineitem_test_schema
int extractSchemaFromString(vector<struct col_info> &schema, string& schema_string) {

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

        // expected num of metadata items in our Tables::col_info struct
        uint32_t col_metadata_items = 5;

        // ignore empty strings after trimming, due to above boost split.
        // expected len of at least n items with n-1 spaces
        uint32_t col_info_string_min_len = (2*col_metadata_items) - 1;
        if (col_info_string.length() < col_info_string_min_len)
            continue;

        boost::split(col_data, col_info_string, boost::is_any_of(" "),
                boost::token_compress_on);

        if (col_data.size() != col_metadata_items)
            return TablesErrCodes::BadColInfoFormat;

        std::string name = col_data[4];
        boost::trim(name);
        const struct col_info ci(col_data[0], col_data[1], col_data[2],
                                 col_data[3], name);
        schema.push_back(ci);
    }
    return 0;
}

void printSkyRootHeader(sky_root_header &r) {
    cout << "\n\n\n[SKYHOOKDB ROOT HEADER (flatbuf)]"<< endl;
    cout << "skyhookdb version: "<< r.skyhook_version << endl;
    cout << "schema version: "<< r.schema_version << endl;
    cout << "table name: "<< r.table_name << endl;
    cout << "schema: \n"<< r.schema << endl;
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

    cout << "\n\n[SKYHOOKDB ROW HEADER (flatbuf)]" << endl;
    cout << "RID: "<< r.RID << endl;

    std::string bitstring = "";
    int64_t val = 0;
    uint64_t bit = 0;
    for(int j = 0; j < (int)r.nullbits.size(); j++) {
        val = r.nullbits.at(j);
        for (uint64_t k=0; k < 8 * sizeof(r.nullbits.at(j)); k++) {
            uint64_t mask =  1 << k;
            ((val&mask)>0) ? bit=1 : bit=0;
            bitstring.append(std::to_string(bit));
        }
        cout << "nullbits ["<< j << "]: val=" << val << ": bits=" << bitstring;
        cout << endl;
        bitstring.clear();
    }
}

// parent print function for skyhook flatbuffer data layout
void printSkyFb(const char* fb, size_t fb_size,
        schema_vec &sch) {

    // get root table ptr
    sky_root_header skyroot = Tables::getSkyRootHeader(fb, fb_size);
    printSkyRootHeader(skyroot);

    // print col metadata (only names for now).
    cout  << "Flatbuffer schema for the following rows:" << endl;
    for (schema_vec::iterator it = sch.begin(); it != sch.end(); ++it) {
        cout << " | " << (*it).name;
        if ((*it).is_key) cout << "(key)";
        if (!(*it).nullable) cout << "(NOT NULL)";
    }

    // print row metadata
    for (uint32_t i = 0; i < skyroot.nrows; i++) {

        if (skyroot.delete_vec.at(i) == 1) continue;  // skip dead rows.

        sky_row_header skyrow = getSkyRowHeader(skyroot.offs->Get(i));
        printSkyRowHeader(skyrow);

        auto row = skyrow.data.AsVector();

        cout << "[SKYHOOKDB ROW DATA (flexbuf)]" << endl;
        for (uint32_t j = 0; j < sch.size(); j++ ) {

            col_info col = sch.at(j);

            // check our nullbit vector
            if (col.nullable) {
                bool is_null = false;
                int pos = col.idx / (8*sizeof(skyrow.nullbits.at(0)));
                int col_bitmask = 1 << col.idx;
                if ((col_bitmask & skyrow.nullbits.at(pos)) != 0)  {
                    is_null =true;
                }
                if (is_null) {
                    cout << "|NULL|";
                    continue;
                }
            }

            // print the col val based on its col type
            // TODO: add bounds check for col.id < flxbuf max index
            // note we use index j here as the col index into this row, since
            // "schema" col.id refers to the col num in the table's schema,
            // which may not be the same as this flatbuf, which could contain
            // a different schema if it is a view/project etc.
            cout << "|";
            switch (col.type) {

                case SkyTypeInt: {
                    int val = row[j].AsInt32();
                    cout << val;
                    break;
                }
                case SkyTypeDouble: {
                    double val = row[j].AsDouble();
                    cout << val;
                    break;
                }
                case SkyTypeChar: {
                    int8_t val = row[j].AsInt8();
                    cout <<  static_cast<char>(val);
                    break;
                }
                case SkyTypeDate: {
                    std::string date = row[j].AsString().str();
                    cout << date;
                    break;
                }
                case SkyTypeString: {
                    std::string s = row[j].AsString().str();
                    cout << s;
                    break;
                }
                default: {
                    // This is for other enum types for aggregations,
                    // e.g. SUM, MAX, MIN....
                    break;
                }
            }
        }
        cout << "|";
    }
    cout << endl;
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
            root->rows(),
            root->nrows());
}

sky_row_header getSkyRowHeader(const Tables::Row* rec) {

    return sky_row_header(
            rec->RID(),
            nullbits_vector (rec->nullbits()->begin(), rec->nullbits()->end()),
            rec->data_flexbuffer_root());
}

} // end namespace Tables
