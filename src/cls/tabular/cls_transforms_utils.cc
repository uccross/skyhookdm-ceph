/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include "fbwriter_fbu_utils.h"
#include "cls_tabular_utils.h"


namespace Tables {

std::vector<std::string> printFlatbufFBURowAsCsv2(
        const char* dataptr,
        const size_t datasz,
        bool print_header,
        bool print_verbose,
        long long int max_to_print) {

    std::vector<std::string> csv_strs;
//    sky_root skyroot = getSkyRoot(dataptr, datasz, SFT_FLATBUF_UNION_ROW);
//    schema_vec sc    = schemaFromString(skyroot.data_schema);
//    assert(!sc.empty());
//
//    if (print_verbose)
//        printSkyRootHeader(skyroot);
//
//    // print header row showing schema
//    if (print_header) {
//        bool first = true;
//        for (schema_vec::iterator it = sc.begin(); it != sc.end(); ++it) {
//            if (!first) std::cout << CSV_DELIM;
//            first = false;
//            std::cout << it->name;
//            if (it->is_key) std::cout << "(key)";
//            if (!it->nullable) std::cout << "(NOT NULL)";
//        }
//        std::cout << std::endl; // newline to start first row.
//    }
//
//    // row printing counter, used with --limit flag
//    long long int counter = 0; //counts rows printed
//
//    for (uint32_t i = 0; i < skyroot.nrows; i++, counter++) {
//        if (counter >= max_to_print) break;
//        if (skyroot.delete_vec.at(i) == 1) continue;  // skip dead rows.
//
//        // get the record struct, then the row data
//        sky_rec_fbu skyrec = getSkyRec_fbu(skyroot, i);
//
//        if (print_verbose)
//            printSkyRecHeader_fbu(skyrec);
//
//        auto curr_rec_data = skyrec.data_fbu_rows;
//
//        // for each col in the row, print a NULL or the col's value/
//        std::string a_csv_str = "";
//        bool first = true;
//        for(unsigned int j = 0; j < sc.size(); j++) {
//            if (!first) a_csv_str += CSV_DELIM;
//            first = false;
//            col_info col = sc.at(j);
//
//            if (col.nullable) {  // check nullbit
//                bool is_null = false;
//                int pos = col.idx / (8*sizeof(skyrec.nullbits.at(0)));
//                int col_bitmask = 1 << col.idx;
//                if ((col_bitmask & skyrec.nullbits.at(pos)) != 0)  {
//                    is_null =true;
//                }
//                if (is_null) {
//                    std::cout << "NULL";
//                    continue;
//                }
//            }
//            switch(col.type) {
//              case SDT_UINT32 : {
//                auto int_col_data =
//                    static_cast< const Tables::SDT_UINT32_FBU* >(curr_rec_data->Get(j));
//                a_csv_str += std::to_string(int_col_data->data()->Get(0));
//                break;
//              }
//              case SDT_UINT64 : {
//                auto int_col_data =
//                    static_cast< const Tables::SDT_UINT64_FBU* >(curr_rec_data->Get(j));
//                a_csv_str += std::to_string(int_col_data->data()->Get(0));
//                break;
//              }
//              case SDT_FLOAT : {
//                auto float_col_data =
//                    static_cast< const Tables::SDT_FLOAT_FBU* >(curr_rec_data->Get(j));
//                a_csv_str += std::to_string(float_col_data->data()->Get(0));
//                break;
//              }
//              case SDT_STRING : {
//                auto string_col_data =
//                    static_cast< const Tables::SDT_STRING_FBU* >(curr_rec_data->Get(j));
//                a_csv_str += string_col_data->data()->Get(0)->str();
//                break;
//              }
//              default :
//                assert (TablesErrCodes::UnknownSkyDataType==0);
//            } //switch
//        } //for loop
//        csv_strs.push_back(a_csv_str);
//        a_csv_str = "";
//    } //for
    return csv_strs;
}

std::string get_schema_data_types_fbu(std::string data_schema) {
    std::string result = "" ;
    std::vector<std::string> result_split_on_newlines;
    boost::split(result_split_on_newlines, data_schema, boost::is_any_of("\n"));
    bool first = true ;
    for(unsigned int i=0; i<result_split_on_newlines.size(); i++) {
        if(result_split_on_newlines[i] == "") break;
        std::vector<std::string> result_split_on_spaces;
        boost::split(result_split_on_spaces,
                     result_split_on_newlines[i],
                     boost::is_any_of(" "));
        auto curr_data_type = result_split_on_spaces[2]; //this index is FRAGILE!!!
        if(!first) result += ",";
        else first = false;
        result += curr_data_type;
    }
    return result ;
}
std::string get_schema_attnames_fbu(std::string data_schema) {
    std::string result = "" ;
    std::vector<std::string> result_split_on_newlines;
    boost::split(result_split_on_newlines, data_schema, boost::is_any_of("\n"));
    bool first = true ;
    for(unsigned int i=0; i<result_split_on_newlines.size(); i++) {
        if(result_split_on_newlines[i] == "") break;
        std::vector<std::string> result_split_on_spaces;
        boost::split(result_split_on_spaces,
                     result_split_on_newlines[i],
                     boost::is_any_of(" "));
        auto curr_attname = result_split_on_spaces[5]; //this index is FRAGILE!!!
        if(!first) result += ",";
        else first = false;
        result += curr_attname;
    }
    return result;
}
std::string get_schema_iskey_fbu(std::string data_schema) {
    std::string result = "" ;
    std::vector<std::string> result_split_on_newlines;
    boost::split(result_split_on_newlines, data_schema, boost::is_any_of("\n"));
    bool first = true ;
    for(unsigned int i=0; i<result_split_on_newlines.size(); i++) {
        if(result_split_on_newlines[i] == "") break;
        std::vector<std::string> result_split_on_spaces;
        boost::split(result_split_on_spaces,
                     result_split_on_newlines[i],
                     boost::is_any_of(" "));
        auto curr_iskey = result_split_on_spaces[3]; //this index is FRAGILE!!!
        if(!first) result += ",";
        else first = false;
        result += curr_iskey;
    }
    return result;
}
std::string get_schema_isnullable_fbu(std::string data_schema) {
    std::string result = "" ;
    std::vector<std::string> result_split_on_newlines;
    boost::split(result_split_on_newlines, data_schema, boost::is_any_of("\n"));
    bool first = true ;
    for(unsigned int i=0; i<result_split_on_newlines.size(); i++) {
        if(result_split_on_newlines[i] == "") break;
        std::vector<std::string> result_split_on_spaces;
        boost::split(result_split_on_spaces,
                     result_split_on_newlines[i],
                     boost::is_any_of(" "));
        auto curr_isnullable= result_split_on_spaces[4]; //this index is FRAGILE!!!
        if(!first) result += ",";
        else first = false;
        result += curr_isnullable;
    }
    return result;
}

int transform_fburows_to_fbucols(const char* fb,
                                 const size_t fb_size,
                                 std::string& errmsg,
                                 flatbuffers::FlatBufferBuilder& flatbldr) {

    int errcode = 0;

    // extract root for table info
    auto root       = Tables::GetRoot_FBU(fb);
    auto nrows      = root->nrows();
    auto table_name = root->table_name();
    auto ncols      = root->ncols();
    auto data_schema = root->data_schema()->str();

    // get the fburows csv
    auto csv_strs = printFlatbufFBURowAsCsv2(
        fb,
        fb_size,
        false,
        false,
        10000000);

    for(unsigned int i=0; i<csv_strs.size();i++)
        std::cout << csv_strs[i] << std::endl;

    // convert the row-oriented csv into fbucols
    // and write the result to ceph
    cmdline_inputs_t inputs;
    inputs.debug             = true;
    inputs.write_type        = "ceph";
    inputs.filename          = "NONE";
    inputs.schema_datatypes  = get_schema_data_types_fbu(data_schema);
    inputs.schema_attnames   = get_schema_attnames_fbu(data_schema);
    inputs.schema_iskey      = get_schema_iskey_fbu(data_schema);
    inputs.schema_isnullable = get_schema_isnullable_fbu(data_schema);

    inputs.table_name        = table_name->str();
    inputs.nrows             = nrows;
    inputs.ncols             = ncols;
    inputs.writeto           = "ceph";
    inputs.targetformat      = "SFT_FLATBUF_UNION_COL";
    inputs.targetoid         = "obj.11111111";
    inputs.targetpool        = "tpchdata";
    inputs.cols_per_fb       = ncols;
    inputs.obj_counter       = 11111111;

    do_write(inputs, 1, nrows, inputs.debug, ".");

    return errcode;
} // transform_fburows_to_fbucols

} // end namespace Tables
