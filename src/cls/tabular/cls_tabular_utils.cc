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


namespace Tables {

int processSkyFb(
    flatbuffers::FlatBufferBuilder& flatbldr,
    schema_vec& data_schema,
    schema_vec& query_schema,
    predicate_vec& preds,
    const char* fb,
    const size_t fb_size,
    std::string& errmsg,
    const std::vector<uint32_t>& row_nums)
{
    int errcode = 0;
    delete_vector dead_rows;
    std::vector<flatbuffers::Offset<Tables::Row>> offs;
    sky_root root = getSkyRoot(fb, fb_size);

    // identify the max col idx, to prevent flexbuf vector oob error
    int col_idx_max = -1;
    for (auto it=data_schema.begin(); it!=data_schema.end(); ++it) {
        if (it->idx > col_idx_max)
            col_idx_max = it->idx;
    }

    bool project_all = std::equal (data_schema.begin(), data_schema.end(),
                                   query_schema.begin(), compareColInfo);

    // build the flexbuf with computed aggregates, aggs are computed for
    // each row that passes, and added to flexbuf after loop below.
    bool encode_aggs = false;
    if (hasAggPreds(preds)) encode_aggs = true;
    bool encode_rows = !encode_aggs;

    // determines if we process specific rows or all rows, since
    // row_nums vector is optional parameter - default process all rows.
    bool process_all_rows = true;
    uint32_t nrows = root.nrows;
    if (!row_nums.empty()) {
        process_all_rows = false;  // process specified row numbers only
        nrows = row_nums.size();
    }

    // 1. check the preds for passing
    // 2a. accumulate agg preds (return flexbuf built after all rows) or
    // 2b. build the return flatbuf inline below from each row's projection
    for (uint32_t i = 0; i < nrows; i++) {

        // process row i or the specified row number
        uint32_t rnum = 0;
        if (process_all_rows) rnum = i;
        else rnum = row_nums[i];
        if (rnum > root.nrows) {
            errmsg += "ERROR: rnum(" + std::to_string(rnum) +
                      ") > root.nrows(" + to_string(root.nrows) + ")";
            return RowIndexOOB;
        }

         // skip dead rows.
        if (root.delete_vec[rnum] == 1) continue;

        // apply predicates
        sky_rec rec = getSkyRec(root.offs->Get(rnum));
        bool pass = applyPredicates(preds, rec);

        if (!pass) continue;  // skip non matching rows.
        if (!encode_rows) continue;  // just continue accumulating agg preds.

        if (project_all) {
            // TODO:  just pass through row table offset to root.offs, do not
            // rebuild row table and flexbuf
        }

        // build the return projection for this row.
        auto row = rec.data.AsVector();
        flexbuffers::Builder *flexbldr = new flexbuffers::Builder();
        flatbuffers::Offset<flatbuffers::Vector<unsigned char>> datavec;
        flexbldr->Vector([&]() {

            // iter over the query schema, locating it within the data schema
            for (auto it=query_schema.begin();
                      it!=query_schema.end() && !errcode; ++it) {

                col_info col = *it;
                if (col.idx < AGG_COL_LAST or col.idx > col_idx_max) {
                    errcode = TablesErrCodes::RequestedColIndexOOB;
                    errmsg.append("ERROR processSkyFb(): table=" +
                            root.table_name + "; rid=" +
                            std::to_string(rec.RID) + " col.idx=" +
                            std::to_string(col.idx) + " OOB.");

                } else {

                    switch(col.type) {  // encode data val into flexbuf

                        case SDT_BOOL:
                            flexbldr->Add(row[col.idx].AsBool());
                            break;
                        case SDT_INT8:
                            flexbldr->Add(row[col.idx].AsInt8());
                            break;
                        case SDT_INT16:
                            flexbldr->Add(row[col.idx].AsInt16());
                            break;
                        case SDT_INT32:
                            flexbldr->Add(row[col.idx].AsInt32());
                            break;
                        case SDT_INT64:
                            flexbldr->Add(row[col.idx].AsInt64());
                            break;
                        case SDT_UINT8:
                            flexbldr->Add(row[col.idx].AsUInt8());
                            break;
                        case SDT_UINT16:
                            flexbldr->Add(row[col.idx].AsUInt16());
                            break;
                        case SDT_UINT32:
                            flexbldr->Add(row[col.idx].AsUInt32());
                            break;
                        case SDT_UINT64:
                            flexbldr->Add(row[col.idx].AsUInt64());
                            break;
                        case SDT_FLOAT:
                            flexbldr->Add(row[col.idx].AsFloat());
                            break;
                        case SDT_DOUBLE:
                            flexbldr->Add(row[col.idx].AsDouble());
                            break;
                        case SDT_CHAR:
                            flexbldr->Add(row[col.idx].AsInt8());
                            break;
                        case SDT_UCHAR:
                            flexbldr->Add(row[col.idx].AsUInt8());
                            break;
                        case SDT_DATE:
                            flexbldr->Add(row[col.idx].AsString().str());
                            break;
                        case SDT_STRING:
                            flexbldr->Add(row[col.idx].AsString().str());
                            break;
                        default: {
                            errcode = TablesErrCodes::UnsupportedSkyDataType;
                            errmsg.append("ERROR processSkyFb(): table=" +
                                    root.table_name + "; rid=" +
                                    std::to_string(rec.RID) + " col.type=" +
                                    std::to_string(col.type) +
                                    " UnsupportedSkyDataType.");
                        }
                    }
                }
            }
        });

        // finalize the row's projected data within our flexbuf
        flexbldr->Finish();

        // build the return ROW flatbuf that contains the flexbuf data
        auto row_data = flatbldr.CreateVector(flexbldr->GetBuffer());
        delete flexbldr;

        // TODO: update nullbits
        auto nullbits = flatbldr.CreateVector(rec.nullbits);
        flatbuffers::Offset<Tables::Row> row_off = \
                Tables::CreateRow(flatbldr, rec.RID, nullbits, row_data);

        // Continue building the ROOT flatbuf's dead vector and rowOffsets vec
        dead_rows.push_back(0);
        offs.push_back(row_off);
    }

    if (encode_aggs) { //  encode each pred agg into return flexbuf.
        PredicateBase* pb;
        flexbuffers::Builder *flexbldr = new flexbuffers::Builder();
        flexbldr->Vector([&]() {
            for (auto itp = preds.begin(); itp != preds.end(); ++itp) {

                // assumes preds appear in same order as return schema
                if (!(*itp)->isGlobalAgg()) continue;
                pb = *itp;
                switch(pb->colType()) {  // encode agg data val into flexbuf
                    case SDT_INT64: {
                        TypedPredicate<int64_t>* p = \
                                dynamic_cast<TypedPredicate<int64_t>*>(pb);
                        int64_t agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        break;
                    }
                    case SDT_UINT64: {
                        TypedPredicate<uint64_t>* p = \
                                dynamic_cast<TypedPredicate<uint64_t>*>(pb);
                        uint64_t agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        break;
                    }
                    case SDT_FLOAT: {
                        TypedPredicate<float>* p = \
                                dynamic_cast<TypedPredicate<float>*>(pb);
                        float agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        break;
                    }
                    case SDT_DOUBLE: {
                        TypedPredicate<double>* p = \
                                dynamic_cast<TypedPredicate<double>*>(pb);
                        double agg_val = p->Val();
                        flexbldr->Add(agg_val);
                        break;
                    }
                    default:  assert(UnsupportedAggDataType==0);
                }
            }
        });
        // finalize the row's projected data within our flexbuf
        flexbldr->Finish();

        // build the return ROW flatbuf that contains the flexbuf data
        auto row_data = flatbldr.CreateVector(flexbldr->GetBuffer());
        delete flexbldr;

        // assume no nullbits in the agg results.  ?
        nullbits_vector nb(2,0);
        auto nullbits = flatbldr.CreateVector(nb);
        int RID = 0;
        flatbuffers::Offset<Tables::Row> row_off = \
                Tables::CreateRow(flatbldr, RID, nullbits, row_data);

        // Continue building the ROOT flatbuf's dead vector and rowOffsets vec
        dead_rows.push_back(0);
        offs.push_back(row_off);
    }

    // now build the return ROOT flatbuf wrapper
    std::string schema_str;
    for (auto it = query_schema.begin(); it != query_schema.end(); ++it) {
        schema_str.append(it->toString() + "\n");
    }
    auto table_name = flatbldr.CreateString(root.table_name);
    auto ret_schema = flatbldr.CreateString(schema_str);
    auto delete_v = flatbldr.CreateVector(dead_rows);
    auto rows_v = flatbldr.CreateVector(offs);
    auto tableOffset = CreateTable(flatbldr, root.skyhook_version,
        root.schema_version, table_name, ret_schema,
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
        s.append(it->toString() + "\n");
    return s;
}

schema_vec schemaFromColNames(schema_vec &current_schema,
                              std::string col_names) {
    schema_vec schema;
    boost::trim(col_names);
    if (col_names == PROJECT_DEFAULT) {
        for (auto it=current_schema.begin(); it!=current_schema.end(); ++it) {
            schema.push_back(*it);
        }
    }
    else if (col_names == RID_INDEX) {
        col_info ci(RID_COL_INDEX, SDT_UINT64, true, false, RID_INDEX);
        schema.push_back(ci);

    }
    else {
        vector<std::string> cols;
        boost::split(cols, col_names, boost::is_any_of(","),
                     boost::token_compress_on);

        // build return schema elems in order of colnames provided.
        for (auto it=cols.begin(); it!=cols.end(); ++it) {
            for (auto it2=current_schema.begin();
                      it2!=current_schema.end(); ++it2) {
                if (it2->compareName(*it))
                    schema.push_back(*it2);
            }
        }
    }
    return schema;
}

// schema string expects the format in cls_tabular_utils.h
// see lineitem_test_schema
schema_vec schemaFromString(std::string schema_string) {

    schema_vec schema;
    vector<std::string> elems;
    boost::split(elems, schema_string, boost::is_any_of("\n"),
                 boost::token_compress_on);

    // assume schema string contains at least one col's info
    if (elems.size() < 1)
        assert (TablesErrCodes::EmptySchema==0);

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

        if (col_data.size() != col_metadata_items)
            assert (TablesErrCodes::BadColInfoFormat==0);

        std::string name = col_data[4];
        boost::trim(name);
        const struct col_info ci(col_data[0], col_data[1], col_data[2],
                                 col_data[3], name);
        schema.push_back(ci);
    }
    return schema;
}

predicate_vec predsFromString(schema_vec &schema, std::string preds_string) {
    // format:  ;colname,opname,value;colname,opname,value;...
    // e.g., ;orderkey,eq,5;comment,like,hello world;..

    predicate_vec preds;
    boost::trim(preds_string);  // whitespace
    boost::trim_if(preds_string, boost::is_any_of(PRED_DELIM_OUTER));

    if (preds_string.empty() || preds_string== SELECT_DEFAULT) return preds;

    vector<std::string> pred_items;
    boost::split(pred_items, preds_string, boost::is_any_of(PRED_DELIM_OUTER),
                 boost::token_compress_on);
    vector<std::string> colnames;
    vector<std::string> select_descr;

    Tables::predicate_vec agg_preds;
    for (auto it=pred_items.begin(); it!=pred_items.end(); ++it) {
        boost::split(select_descr, *it, boost::is_any_of(PRED_DELIM_INNER),
                     boost::token_compress_on);

        assert(select_descr.size()==3);  // currently a triple per pred.

        std::string colname = select_descr.at(0);
        std::string opname = select_descr.at(1);
        std::string val = select_descr.at(2);
        boost::to_upper(colname);

        // this only has 1 col and only used to verify input
        schema_vec sv = schemaFromColNames(schema, colname);
        assert (sv.size() > 0);
        col_info ci = sv.at(0);
        int op_type = skyOpTypeFromString(opname);

        switch (ci.type) {

            case SDT_BOOL: {
                TypedPredicate<bool>* p = \
                        new TypedPredicate<bool> \
                        (ci.idx, ci.type, op_type, std::stol(val));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_INT8: {
                TypedPredicate<int8_t>* p = \
                        new TypedPredicate<int8_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int8_t>(std::stol(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_INT16: {
                TypedPredicate<int16_t>* p = \
                        new TypedPredicate<int16_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int16_t>(std::stol(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_INT32: {
                TypedPredicate<int32_t>* p = \
                        new TypedPredicate<int32_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int32_t>(std::stol(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_INT64: {
                TypedPredicate<int64_t>* p = \
                        new TypedPredicate<int64_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<int64_t>(std::stoll(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_UINT8: {
                TypedPredicate<uint8_t>* p = \
                        new TypedPredicate<uint8_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<uint8_t>(std::stoul(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_UINT16: {
                TypedPredicate<uint16_t>* p = \
                        new TypedPredicate<uint16_t> \
                        (ci.idx, ci.type, op_type,
                        static_cast<uint16_t>(std::stoul(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_UINT32: {
                TypedPredicate<uint32_t>* p = \
                        new TypedPredicate<uint32_t> \
                        (ci.idx, ci.type, op_type,
                        static_cast<uint32_t>(std::stoul(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_UINT64: {
                TypedPredicate<uint64_t>* p = \
                        new TypedPredicate<uint64_t> \
                        (ci.idx, ci.type, op_type, \
                        static_cast<uint64_t>(std::stoull(val)));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_FLOAT: {
                TypedPredicate<float>* p = \
                        new TypedPredicate<float> \
                        (ci.idx, ci.type, op_type, std::stof(val));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_DOUBLE: {
                TypedPredicate<double>* p = \
                        new TypedPredicate<double> \
                        (ci.idx, ci.type, op_type, std::stod(val));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_CHAR: {
                TypedPredicate<char>* p = \
                        new TypedPredicate<char> \
                        (ci.idx, ci.type, op_type, std::stol(val));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_UCHAR: {
                TypedPredicate<unsigned char>* p = \
                        new TypedPredicate<unsigned char> \
                        (ci.idx, ci.type, op_type, std::stoul(val));
                if (p->isGlobalAgg()) agg_preds.push_back(p);
                else preds.push_back(p);
                break;
            }
            case SDT_STRING: {
                TypedPredicate<std::string>* p = \
                        new TypedPredicate<std::string> \
                        (ci.idx, ci.type, op_type, val);
                preds.push_back(p);
                break;
            }
            default: assert (TablesErrCodes::UnknownSkyDataType==0);
        }
    }

    // add agg preds to end so they are only updated if all other preds pass.
    // currently in apply_predicates they are applied in order.
    if (!agg_preds.empty()) {
        preds.reserve(preds.size() + agg_preds.size());
        std::move(agg_preds.begin(), agg_preds.end(),
                  std::inserter(preds, preds.end()));
        agg_preds.clear();
        agg_preds.shrink_to_fit();
    }
    return preds;
}

std::vector<std::string> colnamesFromPreds(predicate_vec &preds,
                                           schema_vec &schema) {
    std::vector<std::string> colnames;
    for (auto it_prd=preds.begin(); it_prd!=preds.end(); ++it_prd) {
        for (auto it_scm=schema.begin(); it_scm!=schema.end(); ++it_scm) {
            if ((*it_prd)->colIdx() == it_scm->idx) {
                colnames.push_back(it_scm->name);
            }
        }
    }
    return colnames;
}

std::vector<std::string> colnamesFromSchema(schema_vec &schema) {
    std::vector<std::string> colnames;
    for (auto it = schema.begin(); it != schema.end(); ++it) {
        colnames.push_back(it->name);
    }
    return colnames;
}

std::string predsToString(predicate_vec &preds, schema_vec &schema) {
    // output format:  "|orderkey,lt,5|comment,like,he|extendedprice,gt,2.01|"
    // where '|' and ',' are denoted as PRED_DELIM_OUTER and PRED_DELIM_INNER

    std::string preds_str;

    // for each pred specified, we iterate over the schema to find its
    // correpsonding column index so we can build the col value string
    // based on col type.
    for (auto it_prd = preds.begin(); it_prd != preds.end(); ++it_prd) {
        for (auto it_sch = schema.begin(); it_sch != schema.end(); ++it_sch) {
            col_info ci = *it_sch;

            // if col indexes match then build the value string.
            if (((*it_prd)->colIdx() == ci.idx) or
                ((*it_prd)->colIdx() == RID_COL_INDEX)) {
                preds_str.append(PRED_DELIM_OUTER);
                std::string colname;

                // set the column name string
                if ((*it_prd)->colIdx() == RID_COL_INDEX)
                    colname = RID_INDEX;  // special col index for RID 'col'
                else
                    colname = ci.name;
                preds_str.append(colname);
                preds_str.append(PRED_DELIM_INNER);
                preds_str.append(skyOpTypeToString((*it_prd)->opType()));
                preds_str.append(PRED_DELIM_INNER);

                // set the col's value as string based on data type
                std::string val;
                switch ((*it_prd)->colType()) {

                    case SDT_BOOL: {
                        TypedPredicate<bool>* p = \
                            dynamic_cast<TypedPredicate<bool>*>(*it_prd);
                        val = std::string(1, p->Val());
                        break;
                    }
                    case SDT_INT8: {
                        TypedPredicate<int8_t>* p = \
                            dynamic_cast<TypedPredicate<int8_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_INT16: {
                        TypedPredicate<int16_t>* p = \
                            dynamic_cast<TypedPredicate<int16_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_INT32: {
                        TypedPredicate<int32_t>* p = \
                            dynamic_cast<TypedPredicate<int32_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_INT64: {
                        TypedPredicate<int64_t>* p = \
                            dynamic_cast<TypedPredicate<int64_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_UINT8: {
                        TypedPredicate<uint8_t>* p = \
                            dynamic_cast<TypedPredicate<uint8_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_UINT16: {
                        TypedPredicate<uint16_t>* p = \
                            dynamic_cast<TypedPredicate<uint16_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_UINT32: {
                        TypedPredicate<uint32_t>* p = \
                            dynamic_cast<TypedPredicate<uint32_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_UINT64: {
                        TypedPredicate<uint64_t>* p = \
                            dynamic_cast<TypedPredicate<uint64_t>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_CHAR: {
                        TypedPredicate<char>* p = \
                            dynamic_cast<TypedPredicate<char>*>(*it_prd);
                        val = std::string(1, p->Val());
                        break;
                    }
                    case SDT_UCHAR: {
                        TypedPredicate<unsigned char>* p = \
                            dynamic_cast<TypedPredicate<unsigned char>*>(*it_prd);
                        val = std::string(1, p->Val());
                        break;
                    }
                    case SDT_FLOAT: {
                        TypedPredicate<float>* p = \
                            dynamic_cast<TypedPredicate<float>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_DOUBLE: {
                        TypedPredicate<double>* p = \
                            dynamic_cast<TypedPredicate<double>*>(*it_prd);
                        val = std::to_string(p->Val());
                        break;
                    }
                    case SDT_STRING: {
                        TypedPredicate<std::string>* p = \
                            dynamic_cast<TypedPredicate<std::string>*>(*it_prd);
                        val = p->Val();
                        break;
                    }
                    default: assert (!val.empty());
                }
                preds_str.append(val);
            }
            if ((*it_prd)->colIdx() == RID_COL_INDEX)
                break;  // only 1 RID col in the schema
        }
    }
    preds_str.append(PRED_DELIM_OUTER);
    return preds_str;
}

int skyOpTypeFromString(std::string op) {
    int op_type = 0;
    if (op=="lt") op_type = SOT_lt;
    else if (op=="gt") op_type = SOT_gt;
    else if (op=="eq") op_type = SOT_eq;
    else if (op=="ne") op_type = SOT_ne;
    else if (op=="leq") op_type = SOT_leq;
    else if (op=="geq") op_type = SOT_geq;
    else if (op=="add") op_type = SOT_add;
    else if (op=="sub") op_type = SOT_sub;
    else if (op=="mul") op_type = SOT_mul;
    else if (op=="div") op_type = SOT_div;
    else if (op=="min") op_type = SOT_min;
    else if (op=="max") op_type = SOT_max;
    else if (op=="sum") op_type = SOT_sum;
    else if (op=="cnt") op_type = SOT_cnt;
    else if (op=="like") op_type = SOT_like;
    else if (op=="in") op_type = SOT_in;
    else if (op=="not_in") op_type = SOT_not_in;
    else if (op=="between") op_type = SOT_between;
    else if (op=="logical_or") op_type = SOT_logical_or;
    else if (op=="logical_and") op_type = SOT_logical_and;
    else if (op=="logical_not") op_type = SOT_logical_not;
    else if (op=="logical_nor") op_type = SOT_logical_nor;
    else if (op=="logical_xor") op_type = SOT_logical_xor;
    else if (op=="logical_nand") op_type = SOT_logical_nand;
    else if (op=="bitwise_and") op_type = SOT_bitwise_and;
    else if (op=="bitwise_or") op_type = SOT_bitwise_or;
    else assert (TablesErrCodes::OpNotRecognized==0);
    return op_type;
}

std::string skyOpTypeToString(int op) {
    std::string op_str;
    if (op==SOT_lt) op_str = "lt";
    else if (op==SOT_gt) op_str = "gt";
    else if (op==SOT_eq) op_str = "eq";
    else if (op==SOT_ne) op_str = "ne";
    else if (op==SOT_leq) op_str = "leq";
    else if (op==SOT_geq) op_str = "geq";
    else if (op==SOT_add) op_str = "add";
    else if (op==SOT_sub) op_str = "sub";
    else if (op==SOT_mul) op_str = "mul";
    else if (op==SOT_div) op_str = "div";
    else if (op==SOT_min) op_str = "min";
    else if (op==SOT_max) op_str = "max";
    else if (op==SOT_sum) op_str = "sum";
    else if (op==SOT_cnt) op_str = "cnt";
    else if (op==SOT_like) op_str = "like";
    else if (op==SOT_in) op_str = "in";
    else if (op==SOT_not_in) op_str = "not_in";
    else if (op==SOT_between) op_str = "between";
    else if (op==SOT_logical_or) op_str = "logical_or";
    else if (op==SOT_logical_and) op_str = "logical_and";
    else if (op==SOT_logical_not) op_str = "logical_not";
    else if (op==SOT_logical_nor) op_str = "logical_nor";
    else if (op==SOT_logical_xor) op_str = "logical_xor";
    else if (op==SOT_logical_nand) op_str = "logical_nand";
    else if (op==SOT_bitwise_and) op_str = "bitwise_and";
    else if (op==SOT_bitwise_or) op_str = "bitwise_or";
    else assert (!op_str.empty());
    return op_str;
}

void printSkyRootHeader(sky_root &r) {
    std::cout << "\n\n\n[SKYHOOKDB ROOT HEADER (flatbuf)]"<< std::endl;
    std::cout << "skyhookdb version: "<< r.skyhook_version << std::endl;
    std::cout << "schema version: "<< r.schema_version << std::endl;
    std::cout << "table name: "<< r.table_name << std::endl;
    std::cout << "schema_name: \n"<< r.schema_name << std::endl;
    std::cout << "delete vector: [";
    for (int i=0; i< (int)r.delete_vec.size(); i++) {
        std::cout << (int)r.delete_vec[i];
        if (i != (int)r.delete_vec.size()-1)
            std::cout <<", ";
    }
    std::cout << "]" << std::endl;
    std::cout << "nrows: " << r.nrows << std::endl;
    std::cout << std::endl;
}

void printSkyRecHeader(sky_rec &r) {

    std::cout << "\n\n[SKYHOOKDB ROW HEADER (flatbuf)]" << std::endl;
    std::cout << "RID: "<< r.RID << std::endl;

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
        std::cout << "nullbits ["<< j << "]: val=" << val << ": bits="
                  << bitstring;
        std::cout << std::endl;
        bitstring.clear();
    }
}

// parent print function for skyhook flatbuffer data layout
void printSkyFb(const char* fb, size_t fb_size, schema_vec &sch) {

    // get root table ptr
    sky_root skyroot = Tables::getSkyRoot(fb, fb_size);
    if (skyroot.nrows == 0) return;  // nothing to see here...

    printSkyRootHeader(skyroot);

    // print col metadata (only names for now).
    std::cout  << "Flatbuffer schema for the following rows:" << std::endl;
    for (schema_vec::iterator it = sch.begin(); it != sch.end(); ++it) {
        std::cout << " | " << it->name;
        if (it->is_key) std::cout << "(key)";
        if (!it->nullable) std::cout << "(NOT NULL)";
    }

    // print row metadata
    std::cout << "\nskyroot.nrows=" << skyroot.nrows << endl;
    for (uint32_t i = 0; i < skyroot.nrows; i++) {

        if (skyroot.delete_vec.at(i) == 1) continue;  // skip dead rows.

        sky_rec skyrec = getSkyRec(skyroot.offs->Get(i));
        printSkyRecHeader(skyrec);

        auto row = skyrec.data.AsVector();

        std::cout << "[SKYHOOKDB ROW DATA (flexbuf)]" << std::endl;
        for (uint32_t j = 0; j < sch.size(); j++ ) {

            col_info col = sch.at(j);

            // check our nullbit vector
            if (col.nullable) {
                bool is_null = false;
                int pos = col.idx / (8*sizeof(skyrec.nullbits.at(0)));
                int col_bitmask = 1 << col.idx;
                if ((col_bitmask & skyrec.nullbits.at(pos)) != 0)  {
                    is_null =true;
                }
                if (is_null) {
                    std::cout << "|NULL|";
                    continue;
                }
            }

            // print the col val based on its col type
            // TODO: add bounds check for col.id < flxbuf max index
            // note we use index j here as the col index into this row, since
            // "schema" col.id refers to the col num in the table's schema,
            // which may not be the same as this flatbuf, which could contain
            // a different schema if it is a view/project etc.
            std::cout << "|";
            switch (col.type) {
                case SDT_BOOL: std::cout << row[j].AsBool(); break;
                case SDT_INT8: std::cout << row[j].AsInt8(); break;
                case SDT_INT16: std::cout << row[j].AsInt16(); break;
                case SDT_INT32: std::cout << row[j].AsInt32(); break;
                case SDT_INT64: std::cout << row[j].AsInt64(); break;
                case SDT_UINT8: std::cout << row[j].AsUInt8(); break;
                case SDT_UINT16: std::cout << row[j].AsUInt16(); break;
                case SDT_UINT32: std::cout << row[j].AsUInt32(); break;
                case SDT_UINT64: std::cout << row[j].AsUInt64(); break;
                case SDT_FLOAT: std::cout << row[j].AsFloat(); break;
                case SDT_DOUBLE: std::cout << row[j].AsDouble(); break;
                case SDT_CHAR: std::cout <<
                    std::string(1, row[j].AsInt8()); break;
                case SDT_UCHAR: std::cout <<
                    std::string(1, row[j].AsUInt8()); break;
                case SDT_DATE: std::cout <<
                    row[j].AsString().str(); break;
                case SDT_STRING: std::cout <<
                    row[j].AsString().str(); break;
                default: assert (TablesErrCodes::UnknownSkyDataType==0);
            }
        }
        std::cout << "|";
    }
    std::cout << std::endl;
}

sky_root getSkyRoot(const char *fb, size_t fb_size) {

    const Table* root = GetTable(fb);

    return sky_root (
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

sky_rec getSkyRec(const Tables::Row* rec) {

    return sky_rec(
            rec->RID(),
            nullbits_vector (rec->nullbits()->begin(), rec->nullbits()->end()),
                             rec->data_flexbuffer_root()
    );
}


bool hasAggPreds(predicate_vec &preds) {
    for (auto it=preds.begin(); it!=preds.end();++it)
        if ((*it)->isGlobalAgg()) return true;
    return false;
}

bool applyPredicates(predicate_vec& pv, sky_rec& rec) {

    bool pass = true;
    auto row = rec.data.AsVector();

    for (auto it = pv.begin(); it != pv.end(); ++it) {
        if (!pass) break;

        switch((*it)->colType()) {

            // NOTE: predicates have typed ints but our int comparison
            // functions are defined on 64bit ints.
            case SDT_BOOL: {
                TypedPredicate<bool>* p = \
                        dynamic_cast<TypedPredicate<bool>*>(*it);
                bool rowval = row[p->colIdx()].AsInt64();
                pass &= compare(rowval,p->Val(),p->opType());
                break;
            }
            case SDT_INT8: {
                TypedPredicate<int8_t>* p = \
                        dynamic_cast<TypedPredicate<int8_t>*>(*it);
                int8_t rowval = row[p->colIdx()].AsInt8();
                int8_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,
                                    static_cast<int64_t>(predval),p->opType());
                break;
            }
            case SDT_INT16: {
                TypedPredicate<int16_t>* p = \
                        dynamic_cast<TypedPredicate<int16_t>*>(*it);
                int16_t rowval = row[p->colIdx()].AsInt16();
                int16_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,
                                    static_cast<int64_t>(predval),p->opType());
                break;
            }
            case SDT_INT32: {
                TypedPredicate<int32_t>* p = \
                        dynamic_cast<TypedPredicate<int32_t>*>(*it);
                int32_t rowval = row[p->colIdx()].AsInt32();
                int32_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,
                                    static_cast<int64_t>(predval),p->opType());
                break;
            }
            case SDT_INT64: {
                TypedPredicate<int64_t>* p = \
                        dynamic_cast<TypedPredicate<int64_t>*>(*it);
                int64_t rowval = 0;
                if ((*it)->colIdx() == RID_COL_INDEX)
                    rowval = rec.RID;  // RID val not in the row
                else
                    rowval = row[p->colIdx()].AsInt64();
                int64_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,predval,p->opType());
                break;
            }
            case SDT_UINT8: {
                TypedPredicate<uint8_t>* p = \
                        dynamic_cast<TypedPredicate<uint8_t>*>(*it);
                uint8_t rowval = row[p->colIdx()].AsUInt8();
                uint8_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,
                                    static_cast<uint64_t>(predval),
                                    p->opType());
                break;
            }
            case SDT_UINT16: {
                TypedPredicate<uint16_t>* p = \
                        dynamic_cast<TypedPredicate<uint16_t>*>(*it);
                uint16_t rowval = row[p->colIdx()].AsUInt16();
                uint16_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,
                                    static_cast<uint64_t>(predval),
                                    p->opType());
                break;
            }
            case SDT_UINT32: {
                TypedPredicate<uint32_t>* p = \
                        dynamic_cast<TypedPredicate<uint32_t>*>(*it);
                uint32_t rowval = row[p->colIdx()].AsUInt32();
                uint32_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,
                                    static_cast<uint64_t>(predval),
                                    p->opType());
                break;
            }
            case SDT_UINT64: {
                TypedPredicate<uint64_t>* p = \
                        dynamic_cast<TypedPredicate<uint64_t>*>(*it);
                uint64_t rowval = 0;
                if ((*it)->colIdx() == RID_COL_INDEX) // RID val not in the row
                    rowval = rec.RID;
                else
                    rowval = row[p->colIdx()].AsUInt64();
                uint64_t predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,predval,p->opType());
                break;
            }
            case SDT_FLOAT: {
                TypedPredicate<float>* p= \
                        dynamic_cast<TypedPredicate<float>*>(*it);
                float rowval = row[p->colIdx()].AsFloat();
                float predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,
                                    static_cast<double>(predval),p->opType());
                break;
            }
            case SDT_DOUBLE: {
                TypedPredicate<double>* p= \
                        dynamic_cast<TypedPredicate<double>*>(*it);
                double rowval = row[p->colIdx()].AsDouble();
                double predval = p->Val();
                if (p->isGlobalAgg())
                    p->updateAgg(computeAgg(rowval,predval,p->opType()));
                else
                    pass &= compare(rowval,predval,p->opType());
                break;
            }
            case SDT_DATE: {
                TypedPredicate<std::string>* p= \
                        dynamic_cast<TypedPredicate<std::string>*>(*it);
                string rowval = row[p->colIdx()].AsString().str();
                pass &= compare(rowval,p->Val(),p->opType());
                break;
            }
            case SDT_CHAR: {
                TypedPredicate<char>* p= \
                        dynamic_cast<TypedPredicate<char>*>(*it);
                if (p->opType() == SOT_like) {  // regex on strings
                    std::string rowval = row[p->colIdx()].AsString().str();
                    std::string predval = std::to_string(p->Val());
                    pass &= compare(rowval,predval,p->opType());
                }
                else {  // int val comparision
                    int8_t rowval = row[p->colIdx()].AsInt8();
                    int8_t predval = p->Val();
                    if (p->isGlobalAgg())
                        p->updateAgg(computeAgg(rowval,predval,p->opType()));
                    else
                        pass &= compare(rowval,
                                        static_cast<int64_t>(predval),
                                        p->opType());
                }
                break;
            }
            case SDT_UCHAR: {
                TypedPredicate<unsigned char>* p= \
                        dynamic_cast<TypedPredicate<unsigned char>*>(*it);
                if (p->opType() == SOT_like) { // regex on strings
                    std::string rowval = row[p->colIdx()].AsString().str();
                    std::string predval = std::to_string(p->Val());
                    pass &= compare(rowval,predval,p->opType());
                }
                else {  // int val comparision
                    uint8_t rowval = row[p->colIdx()].AsUInt8();
                    uint8_t predval = p->Val();
                    if (p->isGlobalAgg())
                        p->updateAgg(computeAgg(rowval,predval,p->opType()));
                    else
                        pass &= compare(rowval,
                                        static_cast<uint64_t>(predval),
                                        p->opType());
                }
                break;
            }
            case SDT_STRING: {
                TypedPredicate<std::string>* p= \
                        dynamic_cast<TypedPredicate<std::string>*>(*it);
                string rowval = row[p->colIdx()].AsString().str();
                pass &= compare(rowval,p->Val(),p->opType());
                break;
            }
            default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
        }
    }
    return pass;
}

bool compare(const int64_t& val1, const int64_t& val2, const int& op) {
    switch (op) {
        case SOT_lt: return val1 < val2;
        case SOT_gt: return val1 > val2;
        case SOT_eq: return val1 == val2;
        case SOT_ne: return val1 != val2;
        case SOT_leq: return val1 <= val2;
        case SOT_geq: return val1 >= val2;
        case SOT_logical_or: return val1 || val2;  // for predicate chaining
        case SOT_logical_and: return val1 && val2;
        case SOT_logical_not: return !val1 && !val2;  // not either, i.e., nor
        case SOT_logical_nor: return !(val1 || val2);
        case SOT_logical_nand: return !(val1 && val2);
        case SOT_logical_xor: return (val1 || val2) && (val1 != val2);
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const uint64_t& val1, const uint64_t& val2, const int& op) {
    switch (op) {
        case SOT_lt: return val1 < val2;
        case SOT_gt: return val1 > val2;
        case SOT_eq: return val1 == val2;
        case SOT_ne: return val1 != val2;
        case SOT_leq: return val1 <= val2;
        case SOT_geq: return val1 >= val2;
        case SOT_logical_or: return val1 || val2;  // for predicate chaining
        case SOT_logical_and: return val1 && val2;
        case SOT_logical_not: return !val1 && !val2;  // not either, i.e., nor
        case SOT_logical_nor: return !(val1 || val2);
        case SOT_logical_nand: return !(val1 && val2);
        case SOT_logical_xor: return (val1 || val2) && (val1 != val2);
        case SOT_bitwise_and: return val1 & val2;
        case SOT_bitwise_or: return val1 | val2;
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const double& val1, const double& val2, const int& op) {
    switch (op) {
        case SOT_lt: return val1 < val2;
        case SOT_gt: return val1 > val2;
        case SOT_eq: return val1 == val2;
        case SOT_ne: return val1 != val2;
        case SOT_leq: return val1 <= val2;
        case SOT_geq: return val1 >= val2;
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const bool& val1, const bool& val2, const int& op) {
    switch (op) {
        case SOT_lt: return val1 < val2;
        case SOT_gt: return val1 > val2;
        case SOT_eq: return val1 == val2;
        case SOT_ne: return val1 != val2;
        case SOT_leq: return val1 <= val2;
        case SOT_geq: return val1 >= val2;
        case SOT_logical_or: return val1 || val2;  // for predicate chaining
        case SOT_logical_and: return val1 && val2;
        case SOT_logical_not: return !val1 && !val2;  // not either, i.e., nor
        case SOT_logical_nor: return !(val1 || val2);
        case SOT_logical_nand: return !(val1 && val2);
        case SOT_logical_xor: return (val1 || val2) && (val1 != val2);
        case SOT_bitwise_and: return val1 & val2;
        case SOT_bitwise_or: return val1 | val2;
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
    return false;  // should be unreachable
}

bool compare(const std::string& val1, const re2::RE2& regx, const int& op) {
    switch (op) {
        case SOT_like:
            return RE2::PartialMatch(val1, regx);
        default: assert (TablesErrCodes::PredicateComparisonNotDefined==0);
    }
   return false;  // should be unreachable
}

std::string buildKeyData(int data_type, uint64_t new_data) {
    std::string data_str = u64tostr(new_data);
    int len = data_str.length();
    int pos = 0;  // pos in u64string to get the minimum keychars for type
    switch (data_type) {
        case SDT_BOOL:
            pos = len-1;
            break;
        case SDT_CHAR:
        case SDT_UCHAR:
        case SDT_INT8:
        case SDT_UINT8:
            pos = len-3;
            break;
        case SDT_INT16:
        case SDT_UINT16:
            pos = len-5;
            break;
        case SDT_INT32:
        case SDT_UINT32:
            pos = len-10;
            break;
        case SDT_INT64:
        case SDT_UINT64:
            pos = 0;
            break;
    }
    return data_str.substr(pos, len);
}

std::string buildKeyPrefix(
        int idx_type,
        std::string schema_name,
        std::string table_name,
        std::vector<string> colnames) {

    boost::trim(schema_name);
    boost::trim(table_name);

    std::string idx_type_str;
    std::string key_cols_str;

    if (schema_name.empty())
        schema_name = SCHEMA_NAME_DEFAULT;

    if (table_name.empty())
        table_name = TABLE_NAME_DEFAULT;

    if (colnames.empty())
        key_cols_str = IDX_KEY_COLS_DEFAULT;

    switch (idx_type) {
    case SIT_IDX_FB:
        idx_type_str = SkyIdxTypeMap.at(SIT_IDX_FB);
        break;
    case SIT_IDX_RID:
        idx_type_str = SkyIdxTypeMap.at(SIT_IDX_RID);
        for (unsigned i = 0; i < colnames.size(); i++) {
            if (i > 0) key_cols_str += Tables::IDX_KEY_DELIM_INNER;
            key_cols_str += colnames[i];
        }
        break;
    case SIT_IDX_REC:
        idx_type_str =  SkyIdxTypeMap.at(SIT_IDX_REC);
        // stitch the colnames together
        for (unsigned i = 0; i < colnames.size(); i++) {
            if (i > 0) key_cols_str += Tables::IDX_KEY_DELIM_INNER;
            key_cols_str += colnames[i];
        }
        break;
    case SIT_IDX_TXT:
        idx_type_str =  SkyIdxTypeMap.at(SIT_IDX_TXT);
        break;
    default:
        idx_type_str = "IDX_UNK";
    }

    // TODO: this prefix should be encoded as a unique index number
    // to minimize key length/redundancy across keys
    return (
        idx_type_str + IDX_KEY_DELIM_OUTER +
        schema_name + IDX_KEY_DELIM_INNER +
        table_name + IDX_KEY_DELIM_OUTER +
        key_cols_str + IDX_KEY_DELIM_OUTER
    );
}

/*
 * Given a predicate vector, check if the opType provided is present therein.
   Used to compare idx ops, for special handling of leq case, etc.
*/
bool check_predicate_ops(predicate_vec index_preds, int opType)
{
    for (unsigned i = 0; i < index_preds.size(); i++) {
        if(index_preds[i]->opType() != opType)
            return false;
    }
    return true;
}

bool check_predicate_ops_all_equality(predicate_vec index_preds)
{
    for (unsigned i = 0; i < index_preds.size(); i++) {
        switch (index_preds[i]->opType()) {
            case SOT_eq:
            case SOT_leq:
            case SOT_geq:
                continue;
            default:
                return false;
        }
    }
    return true;
}


// used for index prefix matching during index range queries
bool compare_keys(std::string key1, std::string key2)
{

    // Format of keys is like IDX_REC:*-LINEITEM:LINENUMBER-ORDERKEY:00000000000000000001-00000000000000000006
    // First splitting both the string on the basis of ':' delimiter
    vector<std::string> elems1;
    boost::split(elems1, key1, boost::is_any_of(IDX_KEY_DELIM_OUTER),
                                                boost::token_compress_on);

    vector<std::string> elems2;
    boost::split(elems2, key2, boost::is_any_of(IDX_KEY_DELIM_OUTER),
                                                boost::token_compress_on);

    // 4th entry in vector represents the value vector i.e after prefix
    vector<std::string> value1;
    vector<std::string> value2;

    // Now splitting value field on the basis of '-' delimiter
    boost::split(value1,
                 elems1[IDX_FIELD_Value],
                 boost::is_any_of(IDX_KEY_DELIM_INNER),
                 boost::token_compress_on);
    boost::split(value2,
                 elems2[IDX_FIELD_Value],
                 boost::is_any_of(IDX_KEY_DELIM_INNER),
                 boost::token_compress_on);

   // Compare first token of both field value
    if (!value1.empty() and !value2.empty()) {
        if(value1[0] == value2[0])
            return true;
    }
    return false;
}


} // end namespace Tables
