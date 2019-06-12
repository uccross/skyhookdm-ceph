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

using namespace std;

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
	std::string line;
	std::getline(inFile, line);
	inFile.close();
	// Split by '|' deliminator
	std::vector<std::string> parsedRow = split(line, '|');
	
// ------------------------------------------------------------------------Initialize FlexBuffer ----------------------------------------
	
	//	 Creating temp variables to throw into the builder
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
	//	 Parsing of Date fields	
	vector<int> shipdate,receiptdate,commitdate;
	shipdate = splitDate(parsedRow[10]);
	receiptdate = splitDate(parsedRow[11]);
	commitdate = splitDate(parsedRow[12]);
	//	 String fields
	string shipinstruct = parsedRow[13];
	string shipmode = parsedRow[14];
	string comment = parsedRow[15];
	// Initialize our LINEITEM
	// 	Create a flexbuffer as a map
	flexbuffers::Builder fbb;
	fbb.Map([&]() {
		fbb.UInt("orderkey", orderkey);
		fbb.UInt("partkey", partkey);
		fbb.UInt("suppkey", suppkey);
		fbb.UInt("linenumber", linenumber);
		fbb.Float("quantity", quantity);
		fbb.Float("extendedprice", extendedprice);
		fbb.Float("discount", discount);
		fbb.Float("tax", tax);
		fbb.UInt("returnflag", returnflag);
		fbb.UInt("linestatus", linestatus);
		fbb.Vector("shipdate", [&]() {
			fbb.UInt(shipdate[0]);
			fbb.UInt(shipdate[1]);
			fbb.UInt(shipdate[2]);
		});
		fbb.Vector("receiptdate", [&]() {
			fbb.UInt(receiptdate[0]);
			fbb.UInt(receiptdate[1]);
			fbb.UInt(receiptdate[2]);
		});
		fbb.Vector("commitdate", [&]() {
			fbb.UInt(commitdate[0]);
			fbb.UInt(commitdate[1]);
			fbb.UInt(commitdate[2]);
		});
		fbb.String("shipinstruct", shipinstruct);
		fbb.String("shipmode", shipmode);
		fbb.String("comment", comment);
	});
	fbb.Finish();
	//	Create a flexbuffer as a vector
	flexbuffers::Builder fbb2;
	fbb2.Vector([&]() {
		fbb2.UInt(orderkey);
		fbb2.UInt(partkey);
		fbb2.UInt(suppkey);
		fbb2.UInt(linenumber);
		fbb2.Float(quantity);
		fbb2.Float(extendedprice);
		fbb2.Float(discount);
		fbb2.Float(tax);
		fbb2.UInt(returnflag);
		fbb2.UInt(linestatus);
		fbb2.Vector([&]() {
			fbb2.UInt(shipdate[0]);
			fbb2.UInt(shipdate[1]);
			fbb2.UInt(shipdate[2]);
		});
		fbb2.Vector([&]() {
			fbb2.UInt(receiptdate[0]);
			fbb2.UInt(receiptdate[1]);
			fbb2.UInt(receiptdate[2]);
		});
		fbb2.Vector([&]() {
			fbb2.UInt(commitdate[0]);
			fbb2.UInt(commitdate[1]);
			fbb2.UInt(commitdate[2]);
		});
		fbb2.String(shipinstruct);
		fbb2.String(shipmode);
		fbb2.String(comment);
	});
	fbb2.Finish();

// --------------------------------------------Initialize Temp Variables--------------------------------------------------------------------

	// Get Buffer pointers and check size of buffers
	vector<uint8_t> buf = fbb.GetBuffer();
	int size = fbb.GetSize();
	cout<<"Buffer Size (map): "<<size<<endl;
	vector<uint8_t> buf2 = fbb2.GetBuffer();
	int size2 = fbb2.GetSize();
	cout<<"Buffer Size (vector): "<<size2<<endl<<endl;

	// Initialize temp variables for assertions
	flexbuffers::Builder temp;
	temp.Vector([&]() {
		temp.UInt(100);
		temp.String("temp");
	});
	temp.Finish();
	vector<uint8_t> tempbuf = temp.GetBuffer();
	auto vec = flexbuffers::GetRoot(tempbuf).AsVector();

	int _orderkey, _partkey, _suppkey, _linenumber;
	float _quantity, _extendedprice, _discount, _tax;
	int8_t _returnflag, _linestatus;
	flexbuffers::Vector _shipdate = vec;
	flexbuffers::Vector _commitdate = vec;
	flexbuffers::Vector _receiptdate = vec;
	flexbuffers::String _shipinstruct = vec[1].AsString();
 	flexbuffers::String _shipmode = vec[1].AsString();
	flexbuffers::String _comment = vec[1].AsString();
	
// -------------------------------------------------Start TIMING MAP FLEXBUFFER----------------------------------------------
	struct timeval start, end;
	double t;

	auto map = flexbuffers::GetRoot(buf).AsMap();
	double avg = 0;
	double avg2 = 0;
	int n = 1000000;
	int n2 = 10;
	double minN = 1000000;
	double maxN = 0;
	// READ A ROW using MAP
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			map = flexbuffers::GetRoot(buf).AsMap();
			_orderkey = map["orderkey"].AsUInt32();
			_partkey = map["partkey"].AsUInt32();
			_suppkey = map["suppkey"].AsUInt32();
			_linenumber = map["linenumber"].AsUInt32();
			_quantity = map["quantity"].AsFloat();
			_extendedprice = map["extendedprice"].AsFloat();
			_discount = map["discount"].AsFloat();
			_tax = map["tax"].AsFloat();
			_returnflag = map["returnflag"].AsUInt8();
			_linestatus = map["linestatus"].AsUInt8();
			_shipdate = map["shipdate"].AsVector();
			_receiptdate = map["receiptdate"].AsVector();
			_commitdate = map["commitdate"].AsVector();
			_shipinstruct = map["shipinstruct"].AsString();
			_shipmode = map["shipmode"].AsString();
			_comment = map["comment"].AsString();
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
	std::cout<<"Reading ROW (map) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading ROW: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A INT using MAP	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			map = flexbuffers::GetRoot(buf).AsMap();
			_partkey = map["partkey"].AsUInt32();
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
	std::cout<<"Reading INT (map) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading INT: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A FLOAT using MAP	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			map = flexbuffers::GetRoot(buf).AsMap();
			_extendedprice = map["extendedprice"].AsFloat();
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
	std::cout<<"Reading FLOAT (map) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading FLOAT: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A BYTE using MAP	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			map = flexbuffers::GetRoot(buf).AsMap();
			_linestatus = map["linestatus"].AsUInt8();
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
	std::cout<<"Reading BYTE (map) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading BYTE: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A DATE VEC using MAP	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			map = flexbuffers::GetRoot(buf).AsMap();
			_commitdate = map["commitdate"].AsVector();
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
	std::cout<<"Reading VECTOR (map) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading VECTOR: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A String using MAP	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			map = flexbuffers::GetRoot(buf).AsMap();
			_comment = map["comment"].AsString();
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
	cout<<"Reading String (map) took "<< avg2<<" microseconds over "<<n<<" runs"<<endl;
	cout<<"Reading String: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

// -----------------------------------------CHECK CONTENTS OF MAP FLEXBUFFER----------------------------------	
	assertionCheck(orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, receiptdate, commitdate,
			shipinstruct, shipmode, comment, _orderkey, _partkey, _suppkey, _linenumber, _quantity, _extendedprice, _discount, _tax,
			_returnflag, _linestatus, _shipdate, _receiptdate, _commitdate, _shipinstruct, _shipmode, _comment);

// -------------------------------------------------- START TIMING VECTOR FLEXBUFFER--------------------------------
	// READ A ROW using VECTOR
	auto row = flexbuffers::GetRoot(buf2).AsVector();
	avg = 0;
	avg2 = 0;
	minN = 10000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			row = flexbuffers::GetRoot(buf2).AsVector();
			_orderkey = row[0].AsUInt32();
			_partkey = row[1].AsUInt32();
			_suppkey = row[2].AsUInt32();
			_linenumber = row[3].AsUInt32();
			_quantity = row[4].AsFloat();
			_extendedprice = row[5].AsFloat();
			_discount = row[6].AsFloat();
			_tax = row[7].AsFloat();
			_returnflag = row[8].AsUInt8();
			_linestatus = row[9].AsUInt8();
			_shipdate = row[10].AsVector();
			_receiptdate = row[11].AsVector();
			_commitdate = row[12].AsVector();
			_shipinstruct = row[13].AsString();
			_shipmode = row[14].AsString();
			_comment = row[15].AsString();
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
	cout<<"Reading ROW (Vector) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading ROW: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A INT using VECTOR	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			row= flexbuffers::GetRoot(buf2).AsVector();
			_partkey = row[1].AsUInt32();
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
	std::cout<<"Reading INT (vector) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading INT: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A FLOAT using VECTOR	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			row = flexbuffers::GetRoot(buf2).AsVector();
			_quantity = row[4].AsUInt32();
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
	std::cout<<"Reading FLOAT (vector) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading FLOAT: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A BYTE using VECTOR
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			row = flexbuffers::GetRoot(buf2).AsVector();
			_linestatus = row[9].AsUInt32();
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
	std::cout<<"Reading BYTE (vector) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading BYTE: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A DATE VEC using VECTOR	
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			row = flexbuffers::GetRoot(buf2).AsVector();
			_shipdate = row[10].AsVector();
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
	std::cout<<"Reading VECTOR (vector) took "<< avg2<< " microseconds over "<<n<<" runs"<<std::endl;
	cout<<"Reading VECTOR: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	// READ A String using VECTOR
	avg = 0;
	avg2 = 0;
	minN = 1000000;
	maxN = 0;
	for(int i=0;i<n2;i++) {
		avg = 0;
		gettimeofday(&start, NULL);
		for(int i=0;i<n;i++) {
			row = flexbuffers::GetRoot(buf2).AsVector();
			_comment = row[15].AsString();
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
	cout<<"Reading String (vector) took "<< avg2<<" microseconds over "<<n<<" runs"<<endl;
	cout<<"Reading String: minAccessTime- "<<minN<<" maxAccessTime- "<<maxN<<endl<<endl;

	//	Check the number of fields of flexbuffer (map and vector)
	int mFields = map.size();
	int rFields = row.size();
	cout<<"Map Fields: "<<mFields<<" Vector Fields: "<<rFields<<endl;

// -------------------------------------------------CHECK CONTENTS OF VECTOR FLEXBUFFER -----------------------------------
	assertionCheck(orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, receiptdate, commitdate,
			shipinstruct, shipmode, comment, _orderkey, _partkey, _suppkey, _linenumber, _quantity, _extendedprice, _discount, _tax,
			_returnflag, _linestatus, _shipdate, _receiptdate, _commitdate, _shipinstruct, _shipmode, _comment);
	return 0;
}
// --------------------------------------------------------HELPER FUNCTIONS----------------------------------------------------------
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
