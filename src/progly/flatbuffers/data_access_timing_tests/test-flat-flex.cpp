/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/time.h>
#include "../header_files/flatbuffers/flexbuffers.h"
#include "../header_files/flatflex_generated.h"

using namespace std;
using namespace Tables;

std::vector<std::string> split(const std::string &s, char delim);
std::vector<int> splitDate(const std::string &s);
void assertionCheck(int, int, int, int, float, float, float, float, int8_t, int8_t, vector<int>, vector<int>, vector<int>, string, string, string,
			int, int, int, int, float, float, float, float, int8_t, int8_t,
				flexbuffers::Vector, flexbuffers::Vector, flexbuffers::Vector, flexbuffers::String, flexbuffers::String, flexbuffers::String);

int main()
{

	// Open .tbl file
	std::ifstream inFile;
	inFile.open("lineitem-10K-rows.tbl", std::ifstream::in);
	if(!inFile)
	{
		std::cout<<"Cannot open file!!!";
		return 0;
	}		
	// Get a row
	std::string row;
	std::getline(inFile, row);
	inFile.close();
	// Split by '|' deliminator
	std::vector<std::string> parsedRow = split(row, '|');
	
// -----------------------------------------------------Initialize FlatBuffer ----------------------------------------
	//	 Create a 'FlatBufferBuilder', which will be used to create our LINEITEM FlexBuffers
	flatbuffers::FlatBufferBuilder fbbuilder(1024);
	vector<flatbuffers::Offset<Rows>> rows_vector;
	//	 Creating temp variables to throw into the fbbuilder
	int orderkey = atoi(parsedRow[0].c_str());
	int partkey = atoi(parsedRow[1].c_str());
	int suppkey = atoi(parsedRow[2].c_str());
	int linenumber = atoi(parsedRow[3].c_str());
	float quantity = stof(parsedRow[4]);
	float extendedprice = stof(parsedRow[5]);
	float discount = stof(parsedRow[6]);
	float tax = stof(parsedRow[7]);
	
	int8_t returnflag = (int8_t) atoi(parsedRow[8].c_str());
	int8_t linestatus = (int8_t) atoi(parsedRow[9].c_str());
	//	 Parsing and Serializing of Date fields	
	vector<int> shipdate, receiptdate, commitdate;
	shipdate = splitDate(parsedRow[10]);
	receiptdate = splitDate(parsedRow[11]);
	commitdate = splitDate(parsedRow[12]);
	string shipinstruct = parsedRow[13];
	string shipmode = parsedRow[14];
	string comment = parsedRow[15];

	int rowID = 500;
// -----------------------------------------Create Row 0 FlexBuffer-------------------------------------------
	flexbuffers::Builder flx0;
	flx0.Vector([&]() {
		flx0.UInt(orderkey);
		flx0.UInt(partkey);
		flx0.UInt(suppkey);
		flx0.UInt(linenumber);
		flx0.Float(quantity);
		flx0.Float(extendedprice);
		flx0.Float(discount);
		flx0.Float(tax);
		flx0.UInt(returnflag);
		flx0.UInt(linestatus);
		flx0.Vector([&]() {
			flx0.UInt(shipdate[0]);
			flx0.UInt(shipdate[1]);
			flx0.UInt(shipdate[2]);
		});
		flx0.Vector([&]() {
			flx0.UInt(receiptdate[0]);
			flx0.UInt(receiptdate[1]);
			flx0.UInt(receiptdate[2]);
		});
		flx0.Vector([&]() {
			flx0.UInt(commitdate[0]);
			flx0.UInt(commitdate[1]);
			flx0.UInt(commitdate[2]);
		});
		flx0.String(shipinstruct);
		flx0.String(shipmode);
		flx0.String(comment);
	});
	flx0.Finish();
	// Get Pointer to FlexBuffer Row
	vector<uint8_t> flxPtr = flx0.GetBuffer();
	int flxSize = flx0.GetSize();
	cout<<"FlexBuffer Size: "<<flxSize<<" bytes"<<endl;
// ---------------------------------------Create 4MB of FlexBuffers----------------------------------------
	for(int i=0;i<1;i++) {
		// Serialize buffer into a Flatbuffer::Vector
		auto flxSerial = fbbuilder.CreateVector(flxPtr);
		// Create a Row from FlexBuffer and new ID
		auto row0 = CreateRows(fbbuilder, rowID++, flxSerial);
		// Push new row onto vector of rows
		rows_vector.push_back(row0);
	}
//----------------------------------------Create FlatBuffer of FlexBuffers --------------------------------
	// Serializing vector of Rows adds 12 bytes
	auto rows_vec = fbbuilder.CreateVector(rows_vector);
	auto table = CreateTable(fbbuilder, rows_vec);
	fbbuilder.Finish(table);

// --------------------------------------------Start Timing Rows-------------------------------------------
//
	// Get Pointer to FlatBuffer
	uint8_t *buf = fbbuilder.GetBufferPointer();
	int size = fbbuilder.GetSize();
	cout<<"Buffer Size (FlatBuffer of FlexBuffers): "<<size<<" bytes"<<endl;
	// Check number of FlexBuffers in FlatBuffer
	volatile auto records = GetTable(buf);
	int recsCount = records->data()->size();
	cout<<"Row Count: "<<recsCount<<endl;
	cout<<"Overhead for "<<recsCount<<" rows: "<<(size - (recsCount*flx0.GetSize()) ) / (double)recsCount<<" bytes per row"<<endl<<endl;
	// Initialize temporary variables to store FlexBuffer data
	volatile int32_t _orderkey, _partkey, _suppkey, _linenumber;
	volatile float _quantity, _extendedprice, _discount, _tax;
	volatile int8_t _returnflag, _linestatus;

	// 	FlexBuffers Vectors and Strings need to be Initialized to dummy values
	// 		No flexbuffers::String() or flexbuffers::Vector() constructors
	auto tempflxRoot = records->data()->Get(0)->rows_flexbuffer_root().AsVector();
	flexbuffers::Vector _shipdate = tempflxRoot[11].AsVector();
	flexbuffers::Vector _receiptdate = tempflxRoot[11].AsVector();
	flexbuffers::Vector _commitdate = tempflxRoot[11].AsVector();
	flexbuffers::String _shipinstruct = tempflxRoot[15].AsString();;
	flexbuffers::String _shipmode = tempflxRoot[15].AsString();
	flexbuffers::String _comment = tempflxRoot[15].AsString();

// ------------------------------------------------------------------------------Testing Mutation of FlexBuffer Serialized
	

	// Setup the Timing test
	int rowNum = 0;
	struct timeval start, end;
	double t;

	double avg = 0;
	double avg2 = 0;
	int n = 1000000;
	int n2 = 10;
	double minN = 1000000;
	double maxN = 0;
	// READ A ROW
		for(int i=0;i<n2;i++) {
			avg = 0;
			gettimeofday(&start, NULL);
			for(int j=0;j<n;j++) {
				records = GetTable(buf);
				const flatbuffers::Vector<flatbuffers::Offset<Rows>>* recs = records->data();
				auto flxRoot = recs->Get(rowNum)->rows_flexbuffer_root();
				auto rowVector = flxRoot.AsVector();
				_orderkey = rowVector[0].AsUInt32();
				_partkey = rowVector[1].AsUInt32();
				_suppkey = rowVector[2].AsUInt32();
				_linenumber = rowVector[3].AsUInt32();
				_quantity = rowVector[4].AsFloat();
				_extendedprice = rowVector[5].AsFloat();
				_discount = rowVector[6].AsFloat();
				_tax = rowVector[7].AsFloat();
				_returnflag = rowVector[8].AsUInt8();
				_linestatus = rowVector[9].AsUInt8();
				_shipdate = rowVector[10].AsVector();
				_receiptdate = rowVector[11].AsVector();
				_commitdate = rowVector[12].AsVector();
				_shipinstruct = rowVector[13].AsString();
				_shipmode = rowVector[14].AsString();
				_comment = rowVector[15].AsString();
			}
			gettimeofday(&end, NULL);
			
			t = (double) ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
			avg += t;
			avg /= n;
			minN = minN<avg?minN:avg;
			maxN = maxN>avg?maxN:avg;
			avg2 += avg;
		}
		avg2 /= n2;
	std::cout<<"Reading LINEITEM took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading ROW: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

// ---------------------------- CHECKING CONTENTS OF LINEITEM IN BUFFER 
// 				(this is only if all rows share the same data)
//				Will compare the last FlexBuffer read with the last FlexBuffer written, or in this test, the first FlexBuffer
	assertionCheck(orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, receiptdate, commitdate,
			shipinstruct, shipmode, comment, _orderkey, _partkey, _suppkey, _linenumber, _quantity, _extendedprice, _discount, _tax,
			_returnflag, _linestatus, _shipdate, _receiptdate, _commitdate, _shipinstruct, _shipmode, _comment);

	return 0;
}

std::vector<std::string> split(const std::string &s, char delim) {
	std::istringstream ss(s);
	std::string item;
	std::vector<std::string> tokens;
	while( getline(ss, item, delim)) {
		tokens.push_back(item);
	}
	return tokens;
}
std::vector<int> splitDate(const std::string &s) {
	std::istringstream ss(s);
	std::string item;
	std::vector<int> tokens;
	while( getline(ss, item, '-')) {
		tokens.push_back(atoi(item.c_str()));
	}
	return tokens;
}
void assertionCheck(int orderkey, int partkey, int suppkey, int linenumber, float quantity, float extendedprice, float discount, float tax,
			int8_t returnflag, int8_t linestatus, vector<int> shipdate, vector<int> receiptdate, vector<int> commitdate,
			string shipinstruct, string shipmode, string comment,
			int _orderkey, int _partkey, int _suppkey, int _linenumber, float _quantity, float _extendedprice, float _discount, float _tax,
			int8_t _returnflag, int8_t _linestatus, flexbuffers::Vector _shipdate, flexbuffers::Vector _receiptdate, flexbuffers::Vector _commitdate,
			flexbuffers::String _shipinstruct, flexbuffers::String _shipmode, flexbuffers::String _comment) {

	assert(_orderkey == orderkey);
	assert(_partkey == partkey);
	assert(_suppkey == suppkey);
	assert(_linenumber == linenumber);
	
	assert(_quantity == quantity);
	assert(_extendedprice == extendedprice);
	assert(_discount == discount);
	assert(_tax == tax);

	assert(_returnflag == returnflag);
	assert(_linestatus == linestatus);

	assert(_shipdate[0].AsInt32() == shipdate[0]);
	assert(_shipdate[1].AsInt32() == shipdate[1]);
	assert(_shipdate[2].AsInt32() == shipdate[2]);
	assert(_receiptdate[0].AsInt32() == receiptdate[0]);
	assert(_receiptdate[1].AsInt32() == receiptdate[1]);
	assert(_receiptdate[2].AsInt32() == receiptdate[2]);
	assert(_commitdate[0].AsInt32() == commitdate[0]);
	assert(_commitdate[1].AsInt32() == commitdate[1]);
	assert(_commitdate[2].AsInt32() == commitdate[2]);

	assert( strcmp( _shipinstruct.c_str(), shipinstruct.c_str() ) == 0);
	assert( strcmp( _shipmode.c_str(), shipmode.c_str() ) == 0);
	assert( strcmp( _comment.c_str(), comment.c_str() ) == 0);

}
