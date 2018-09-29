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
#include <time.h>
#include "include/types.h"

#include <boost/lexical_cast.hpp>
#include "re2/re2.h"  // regex matching
#include "objclass/objclass.h"

#include "cls_tabular.h"
#include "flatbuffers/flexbuffers.h"
#include "skyhookv1_generated.h"


namespace Tables {

int processSkyFb(
    flatbuffers::FlatBufferBuilder &flatbldr,
    schema_vec &schema_in,
    schema_vec &schema_out,
    predicate_vec &preds,
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

    bool project_all = std::equal (schema_in.begin(), schema_in.end(),
                                   schema_out.begin(), Tables::compareCi);

    // build the flexbuf for each row's data
    for (uint32_t i = 0; i < skyroot.nrows && !errcode; i++) {

        if (skyroot.delete_vec.at(i) == 1) continue;  // skip dead rows.

        // apply predicates
        sky_row_header skyrow = getSkyRowHeader(skyroot.offs->Get(i));
        bool pass = applyPredicates(preds, skyrow);
        if (!pass) continue;  // skip non matching rows.

        if (project_all) {
            // TODO:  just pass through row table offset to root.offs, do not
            // rebuild row table and flexbuf
        }

        // build the projection for this row.
        auto row = skyrow.data.AsVector();
        flexbuffers::Builder *flexbldr = new flexbuffers::Builder();
        flatbuffers::Offset<flatbuffers::Vector<unsigned char>> datavec;
        flexbldr->Vector([&]() {

            // iter over the output schema, locating it within the input schema
            for (auto it=schema_out.begin();
                      it!=schema_out.end() && !errcode;
                      ++it) {

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
                    case Tables::SkyDataTypeInt8:
                    case Tables::SkyDataTypeInt16:
                    case Tables::SkyDataTypeInt32:
                    case Tables::SkyDataTypeInt64: {
                        int64_t value = row[col.idx].AsInt64();
                        flexbldr->Int(value);
                        break;
                    }
                    case Tables::SkyDataTypeUInt8:
                    case Tables::SkyDataTypeUInt16:
                    case Tables::SkyDataTypeUInt32:
                    case Tables::SkyDataTypeUInt64: {
                        uint64_t value = row[col.idx].AsUInt64();
                        flexbldr->UInt(value);
                        break;
                    }
                    case Tables::SkyDataTypeFloat: {
                        double value = row[col.idx].AsFloat();
                        flexbldr->Float(value);
                        break;
                    }
                    case Tables::SkyDataTypeDouble: {
                        double value = row[col.idx].AsDouble();
                        flexbldr->Double(value);
                        break;
                    }
                    case Tables::SkyDataTypeChar: {
                        int8_t value = row[col.idx].AsInt8();
                        flexbldr->Int(value);
                        break;
                    }
                    case Tables::SkyDataTypeUChar: {
                        uint8_t value = row[col.idx].AsUInt8();
                        flexbldr->UInt(value);
                        break;
                    }
                    case Tables::SkyDataTypeDate: {
                        std::string s = row[col.idx].AsString().str();
                        flexbldr->String(s);
                        break;
                    }
                    case Tables::SkyDataTypeString: {
                        std::string s = row[col.idx].AsString().str();
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
                        assert (TablesErrCodes::UnsupportedSkyDataType==0);
                        break;
                    }}
                }
            }
        });

        // finalize the row's projected data within our flexbuf
        flexbldr->Finish();

        // build the return ROW flatbuf that contains the flexbuf data
        auto row_data = flatbldr.CreateVector(flexbldr->GetBuffer());
        delete flexbldr;

        // TODO: update nullbits
        auto nullbits = flatbldr.CreateVector(skyrow.nullbits);
        flatbuffers::Offset<Tables::Row> row_off = \
                Tables::CreateRow(flatbldr, skyrow.RID, nullbits, row_data);

        // Continue building the ROOT flatbuf's dead vector and rowOffsets vec
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

    // NOTE: the fb may be incomplete/empty, but must finish() else internal
    // fb lib assert finished() fails, hence we must always return a valid fb
    // and catch any ret error code upstream
    flatbldr.Finish(tableOffset);

    return errcode;
}

// simple converstion from schema to its str representation.
std::string schemaToString(schema_vec schema) {
    std::string s;
    for (auto it = schema.begin(); it != schema.end(); ++it)
        s.append((*it).toString() + "\n");
    return s;
}

void schemaFromProjectColsString(schema_vec &ret_schema,
                              schema_vec &current_schema,
                              std::string project_col_names) {

    boost::trim(project_col_names);
    if (project_col_names == "*") {
        for (auto it=current_schema.begin(); it!=current_schema.end(); ++it) {
            ret_schema.push_back(*it);
        }
    }
    else {
        vector<std::string> cols;
        boost::split(cols, project_col_names, boost::is_any_of(","),
                boost::token_compress_on);

        // build return schema elems in order of colnames provided.
        for (auto it=cols.begin(); it!=cols.end(); ++it) {
            for (auto it2=current_schema.begin();
                      it2!=current_schema.end(); ++it2) {
                if ((*it2).compareName(*it))
                    ret_schema.push_back(*it2);
            }
        }
    }
}

// schema string expects the format in cls_tabular_utils.h
// see lineitem_test_schema
void schemaFromString(schema_vec &schema, std::string schema_string) {

    vector<std::string> elems;
    boost::split(elems, schema_string, boost::is_any_of("\n"),
            boost::token_compress_on);

    // assume schema string contains at least one col's info
    if (elems.size() < 1) {
        std::cerr << "ERROR: getSchemaFromSchemaString, Tables::ErrCodes="
                  << TablesErrCodes::EmptySchema << endl;
        assert (TablesErrCodes::EmptySchema==0);
    }

    for (auto it = elems.begin(); it != elems.end(); ++it) {

        vector<std::string> col_data;  // each string describes one col info
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

        if (col_data.size() != col_metadata_items) {
            std::cerr << "ERROR: getSchemaFromSchemaString, Tables::ErrCodes="
                      << TablesErrCodes::BadColInfoFormat << endl;
            assert (TablesErrCodes::BadColInfoFormat==0);
        }

        std::string name = col_data[4];
        boost::trim(name);
        const struct col_info ci(col_data[0], col_data[1], col_data[2],
                                 col_data[3], name);
        schema.push_back(ci);
    }
}

void predsFromString(predicate_vec &preds, schema_vec &schema,
                     std::string preds_string) {
    // format:  ;colname,opname,value;colname,opname,value;...
    // e.g., ;orderkey,eq,5;comment,like,hello world;..

    boost::trim(preds_string);  // whitespace
    boost::trim_if(preds_string, boost::is_any_of(PRED_DELIM_OUTER));

    if (preds_string.empty() || preds_string=="*") return;
    vector<std::string> pred_items;
    boost::split(pred_items, preds_string, boost::is_any_of(PRED_DELIM_OUTER),
                boost::token_compress_on);
    vector<std::string> colnames;
    vector<std::string> select_descr;

    for (auto it=pred_items.begin(); it!=pred_items.end(); ++it) {
        boost::split(select_descr, *it, boost::is_any_of(PRED_DELIM_INNER),
                boost::token_compress_on);
        assert(select_descr.size()==3);  // currently a triple per pred.

        std::string colname = select_descr.at(0);
        std::string opname = select_descr.at(1);
        std::string val = select_descr.at(2);

        schema_vec s;  // this only has 1 col and only used to verify input
        schemaFromProjectColsString(s, schema, colname);
        col_info ci = s.at(0);
        int op_type = skyOpTypeFromString(opname);

        switch (ci.type) {

            case SkyDataType::SkyDataTypeInt8: {
                TypedPredicate<int8_t>* p = \
                        new TypedPredicate<int8_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int8_t>(std::stoi(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeInt16: {
                TypedPredicate<int16_t>* p = \
                        new TypedPredicate<int16_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int16_t>(std::stoi(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeInt32: {
                TypedPredicate<int32_t>* p = \
                        new TypedPredicate<int32_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int32_t>(std::stol(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeInt64: {
                TypedPredicate<int64_t>* p = \
                        new TypedPredicate<int64_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int64_t>(std::stoll(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeUInt8: {
                TypedPredicate<uint8_t>* p = \
                        new TypedPredicate<uint8_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<uint8_t>(std::stoul(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeUInt16: {
                TypedPredicate<uint16_t>* p = \
                        new TypedPredicate<uint16_t> \
                        (ci.idx, ci.type, op_type,
                        static_cast<uint16_t>(std::stoul(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeUInt32: {
                TypedPredicate<uint32_t>* p = \
                        new TypedPredicate<uint32_t> \
                        (ci.idx, ci.type, op_type,
                        static_cast<uint32_t>(std::stoul(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeUInt64: {
                TypedPredicate<uint64_t>* p = \
                        new TypedPredicate<uint64_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<uint64_t>(std::stoull(val)));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeFloat: {
                TypedPredicate<float>* p = \
                        new TypedPredicate<float> \
                        (ci.idx, ci.type, op_type, std::stof(val));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeDouble: {
                TypedPredicate<double>* p = \
                        new TypedPredicate<double> \
                        (ci.idx, ci.type, op_type, std::stod(val));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeChar: {
                TypedPredicate<char>* p = \
                        new TypedPredicate<char> \
                        (ci.idx, ci.type, op_type, std::stol(val));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeUChar: {
                TypedPredicate<unsigned char>* p = \
                        new TypedPredicate<unsigned char> \
                        (ci.idx, ci.type, op_type, std::stoul(val));
                preds.push_back(p);
            } break;

            case SkyDataType::SkyDataTypeString: {
                TypedPredicate<std::string>* p = \
                        new TypedPredicate<std::string> \
                        (ci.idx, ci.type, op_type, val);
                preds.push_back(p);
            } break;

            default: {
                assert (TablesErrCodes::UnknownSkyDataType==0);
                break;
            }
        }
    }
}

std::string predsToString(predicate_vec &preds,  schema_vec &schema) {
    // output format:  "|orderkey,lt,5|comment,like,he|extendedprice,gt,2.01|"
    // where '|' and ',' are denoted as PRED_DELIM_OUTER and PRED_DELIM_INNER
    std::string preds_str;

    for (auto it_prd=preds.begin(); it_prd!=preds.end(); ++it_prd) {
        for (auto it_sch=schema.begin(); it_sch!=schema.end(); ++it_sch) {
            col_info ci = *it_sch;
            if (ci.idx ==  (*it_prd)->colIdx()) {
                preds_str.append(PRED_DELIM_OUTER);
                preds_str.append(ci.name);
                preds_str.append(PRED_DELIM_INNER);
                preds_str.append(skyOpTypeToString((*it_prd)->opType()));
                preds_str.append(PRED_DELIM_INNER);
                std::string val;
                switch (ci.type) {

                    case SkyDataType::SkyDataTypeInt8: {
                        TypedPredicate<int8_t>* p = \
                            dynamic_cast<TypedPredicate<int8_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeInt16: {
                        TypedPredicate<int16_t>* p = \
                            dynamic_cast<TypedPredicate<int16_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeInt32: {
                        TypedPredicate<int32_t>* p = \
                            dynamic_cast<TypedPredicate<int32_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeInt64: {
                        TypedPredicate<int64_t>* p = \
                            dynamic_cast<TypedPredicate<int64_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeUInt8: {
                        TypedPredicate<uint8_t>* p = \
                            dynamic_cast<TypedPredicate<uint8_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeUInt16: {
                        TypedPredicate<uint16_t>* p = \
                            dynamic_cast<TypedPredicate<uint16_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeUInt32: {
                        TypedPredicate<uint32_t>* p = \
                            dynamic_cast<TypedPredicate<uint32_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeUInt64: {
                        TypedPredicate<uint64_t>* p = \
                            dynamic_cast<TypedPredicate<uint64_t>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeChar: {
                        TypedPredicate<char>* p = \
                            dynamic_cast<TypedPredicate<char>*>(*it_prd);
                        val = std::string(1, p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeUChar: {
                        TypedPredicate<unsigned char>* p = \
                            dynamic_cast<TypedPredicate<unsigned char>*>(*it_prd);
                        val = std::string(1, p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeFloat: {
                        TypedPredicate<float>* p = \
                            dynamic_cast<TypedPredicate<float>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeDouble: {
                        TypedPredicate<double>* p = \
                            dynamic_cast<TypedPredicate<double>*>(*it_prd);
                        val = std::to_string(p->getVal());
                        break;
                    }

                    case SkyDataType::SkyDataTypeString: {
                        TypedPredicate<std::string>* p = \
                            dynamic_cast<TypedPredicate<std::string>*>(*it_prd);
                        val = p->getVal();
                        break;
                    }

                    default:
                        assert (!val.empty());
                        break;
                }
                preds_str.append(val);
            }
        }
    }
    preds_str.append(PRED_DELIM_OUTER);
    return preds_str;
}

int skyOpTypeFromString(std::string op) {
    boost::trim(op);
    int op_type = 0;
    if (op=="lt") op_type = SkyOpType::lt;
    else if (op=="gt") op_type = SkyOpType::gt;
    else if (op=="eq") op_type = SkyOpType::eq;
    else if (op=="ne") op_type = SkyOpType::ne;
    else if (op=="leq") op_type = SkyOpType::leq;
    else if (op=="geq") op_type = SkyOpType::geq;
    else if (op=="like") op_type = SkyOpType::like;
    else if (op=="in") op_type = SkyOpType::in;
    else if (op=="not_in") op_type = SkyOpType::not_in;
    else if (op=="between") op_type = SkyOpType::between;
    else if (op=="logical_or") op_type = SkyOpType::logical_or;
    else if (op=="logical_and") op_type = SkyOpType::logical_and;
    else if (op=="logical_not") op_type = SkyOpType::logical_not;
    else if (op=="logical_nor") op_type = SkyOpType::logical_nor;
    else if (op=="logical_xor") op_type = SkyOpType::logical_xor;
    else if (op=="logical_nand") op_type = SkyOpType::logical_nand;
    else if (op=="bitwise_and") op_type = SkyOpType::bitwise_and;
    else if (op=="bitwise_or") op_type = SkyOpType::bitwise_or;
    else assert (TablesErrCodes::OpNotRecognized==0);
    return op_type;
}

std::string skyOpTypeToString(int op) {
    std::string op_str;
    if (op==SkyOpType::lt) op_str = "lt";
    else if (op==SkyOpType::gt) op_str = "gt";
    else if (op==SkyOpType::eq) op_str = "eq";
    else if (op==SkyOpType::ne) op_str = "ne";
    else if (op==SkyOpType::leq) op_str = "leq";
    else if (op==SkyOpType::geq) op_str = "geq";
    else if (op==SkyOpType::like) op_str = "like";
    else if (op==SkyOpType::in) op_str = "in";
    else if (op==SkyOpType::not_in) op_str = "not_in";
    else if (op==SkyOpType::between) op_str = "between";
    else if (op==SkyOpType::logical_or) op_str = "logical_or";
    else if (op==SkyOpType::logical_and) op_str = "logical_and";
    else if (op==SkyOpType::logical_not) op_str = "logical_not";
    else if (op==SkyOpType::logical_nor) op_str = "logical_nor";
    else if (op==SkyOpType::logical_xor) op_str = "logical_xor";
    else if (op==SkyOpType::logical_nand) op_str = "logical_nand";
    else if (op==SkyOpType::bitwise_and) op_str = "bitwise_and";
    else if (op==SkyOpType::bitwise_or) op_str = "bitwise_or";
    else assert (!op_str.empty());
    return op_str;
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

                case SkyDataTypeInt8:
                case SkyDataTypeInt16:
                case SkyDataTypeInt32:
                case SkyDataTypeInt64:
                    cout << row[j].AsInt64();
                    break;

                case SkyDataTypeUInt8:
                case SkyDataTypeUInt16:
                case SkyDataTypeUInt32:
                case SkyDataTypeUInt64:
                    cout << row[j].AsUInt64();
                    break;

                case SkyDataTypeFloat:
                    cout << row[j].AsFloat();
                    break;

                case SkyDataTypeDouble:
                    cout << row[j].AsDouble();
                    break;

                case SkyDataTypeChar:
                    cout << std::string(1, row[j].AsInt8());
                    break;

                case SkyDataTypeUChar:
                    cout << std::string(1, row[j].AsUInt8());
                    break;

                case SkyDataTypeDate:
                    cout << row[j].AsString().str();
                    break;

                case SkyDataTypeString:
                    cout << row[j].AsString().str();
                    break;

                default:
                    // This is for other enum types for aggregations,
                    // e.g. SUM, MAX, MIN....
                    break;
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
            root->nrows()
    );
}

sky_row_header getSkyRowHeader(const Tables::Row* rec) {

    return sky_row_header(
            rec->RID(),
            nullbits_vector (rec->nullbits()->begin(), rec->nullbits()->end()),
                             rec->data_flexbuffer_root()
    );
}

bool applyPredicates(predicate_vec& pv, sky_row_header& row_h) {

    bool pass = true;
    auto row = row_h.data.AsVector();

    for (auto it=pv.begin();it!=pv.end();++it) {
        if (!pass) break;

        switch((*it)->colType()) {

        // NOTE: predicates have typed ints but flexbuffers only uses 64bit
        // ints, so also our comparison functions are defined on 64bit ints.
        case SkyDataTypeInt8:{
            TypedPredicate<int8_t>* p = \
                    dynamic_cast<TypedPredicate<int8_t>*>(*it);
            int64_t rowval = row[p->colIdx()].AsInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeInt16:{
            TypedPredicate<int16_t>* p = \
                    dynamic_cast<TypedPredicate<int16_t>*>(*it);
            int64_t rowval = row[p->colIdx()].AsInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeInt32:{
            TypedPredicate<int32_t>* p = \
                    dynamic_cast<TypedPredicate<int32_t>*>(*it);
            int64_t rowval = row[p->colIdx()].AsInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeInt64: {
            TypedPredicate<int64_t>* p = \
                    dynamic_cast<TypedPredicate<int64_t>*>(*it);
            int64_t rowval = row[p->colIdx()].AsInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeUInt8:{
            TypedPredicate<uint8_t>* p = \
                    dynamic_cast<TypedPredicate<uint8_t>*>(*it);
            uint64_t rowval = row[p->colIdx()].AsUInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeUInt16:{
            TypedPredicate<uint16_t>* p = \
                    dynamic_cast<TypedPredicate<uint16_t>*>(*it);
            uint64_t rowval = row[p->colIdx()].AsUInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeUInt32:{
            TypedPredicate<uint32_t>* p = \
                    dynamic_cast<TypedPredicate<uint32_t>*>(*it);
            uint64_t rowval = row[p->colIdx()].AsUInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeUInt64:{
            TypedPredicate<uint64_t>* p = \
                    dynamic_cast<TypedPredicate<uint64_t>*>(*it);
            uint64_t rowval = row[p->colIdx()].AsUInt64();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeFloat: {
            TypedPredicate<float>* p= \
                    dynamic_cast<TypedPredicate<float>*>(*it);
            float rowval = row[p->colIdx()].AsFloat();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeDouble: {
            TypedPredicate<double>* p= \
                    dynamic_cast<TypedPredicate<double>*>(*it);
            double rowval = row[p->colIdx()].AsDouble();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeDate: {
            TypedPredicate<std::string>* p= \
                    dynamic_cast<TypedPredicate<std::string>*>(*it);
            string rowval = row[p->colIdx()].AsString().str();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        case SkyDataTypeChar: {
            TypedPredicate<char>* p= \
                    dynamic_cast<TypedPredicate<char>*>(*it);
            if (p->opType() == SkyOpType::like) {
                std::string rowval = row[p->colIdx()].AsString().str();
                std::string predval = std::to_string(p->getVal());
                pass &= compare(rowval,predval,p->opType());
            }
            else {
                int64_t rowval = row[p->colIdx()].AsInt64();
                pass &= compare(rowval,p->getVal(),p->opType());
            }
            break;
        }
        case SkyDataTypeUChar: {
            TypedPredicate<unsigned char>* p= \
                    dynamic_cast<TypedPredicate<unsigned char>*>(*it);
            if (p->opType() == SkyOpType::like) {
                std::string rowval = row[p->colIdx()].AsString().str();
                std::string predval = std::to_string(p->getVal());
                pass &= compare(rowval,predval,p->opType());
            }
            else {
                uint64_t rowval = row[p->colIdx()].AsUInt64();
                pass &= compare(rowval,p->getVal(),p->opType());
            }
            break;
        }
        case SkyDataTypeString: {
            TypedPredicate<std::string>* p= \
                    dynamic_cast<TypedPredicate<std::string>*>(*it);
            string rowval = row[p->colIdx()].AsString().str();
            pass &= compare(rowval,p->getVal(),p->opType());
            break;
        }
        default: {
            assert(TablesErrCodes::PredicateComparisonNotDefined==0);
            break;
        }}
    }
    return pass;
}

bool compare(const int64_t& val1, const int64_t& val2, const int& op) {
    switch (op) {
        case lt: return val1 < val2;
        case gt: return val1 > val2;
        case eq: return val1 == val2;
        case ne: return val1 != val2;
        case leq: return val1 <= val2;
        case geq: return val1 >= val2;
        case bitwise_and: return val1 & val2;
        case bitwise_or: return val1 | val2;
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const uint64_t& val1, const uint64_t& val2, const int& op) {
    switch (op) {
        case lt: return val1 < val2;
        case gt: return val1 > val2;
        case eq: return val1 == val2;
        case ne: return val1 != val2;
        case leq: return val1 <= val2;
        case geq: return val1 >= val2;
        case bitwise_and: return val1 & val2;
        case bitwise_or: return val1 | val2;
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const double& val1, const double& val2, const int& op) {
    switch (op) {
        case lt: return val1 < val2;
        case gt: return val1 > val2;
        case eq: return val1 == val2;
        case ne: return val1 != val2;
        case leq: return val1 <= val2;
        case geq: return val1 >= val2;
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const bool& val1, const bool& val2, const int& op) {
    switch (op) {
        case logical_or: return val1 || val2;  // for predicate chaining
        case logical_and: return val1 && val2;
        case logical_not: return !val1 && !val2;  // not either, i.e., nor
        case logical_nor: return !(val1 || val2);
        case logical_nand: return !(val1 && val2);
        case logical_xor: return (val1 || val2) && (val1 != val2);
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const std::string& val1, const re2::RE2& regx, const int& op) {
    switch (op) {
        case like:
            return RE2::PartialMatch(val1, regx);
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
   return false;  // should be unreachable
}

} // end namespace Tables
