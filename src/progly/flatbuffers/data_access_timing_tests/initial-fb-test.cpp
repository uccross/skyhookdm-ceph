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
#include <sys/time.h>
#include <ctime>
#include "../header_files/lineitem_generated.h"

using namespace Tables;

int main()
{
	// Create a 'FlatBufferBuilder', which will be used to create our
	// LINEITEM FlatBuffers
	flatbuffers::FlatBufferBuilder builder(1024);


	// Initial values for our first LINEITEM row
	auto ship_date = Date(1993.0f, 11.0f, 10.0f);
	auto ship_instruct = builder.CreateString("Use lots of tape");
	auto ship_mode = builder.CreateString("First Class Shipping");
	auto comment = builder.CreateString("Double check the shipping address!");

	// If we do not wish to set every field in a table
	// Use this instead of calling CreateLINEITEM()
	LINEITEMBuilder lineitem_builder(builder);
	lineitem_builder.add_L_SHIPDATE(&ship_date);
	lineitem_builder.add_L_SHIPINSTRUCT(ship_instruct);
	lineitem_builder.add_L_SHIPMODE(ship_mode);
	lineitem_builder.add_L_COMMENT(comment);
	auto firstItem = lineitem_builder.Finish();

	// Call 'Finish()' to instruct the builder that this LINEITEM Is complete
	// Regardless of how 'firstItem' was created, we still need to call
	// 'Finish()' on the 'FlatBufferBuilder'
	builder.Finish(firstItem); // Could also call 'FinishLINEITEMBuffer(builder, firstItem);

	struct timeval start, end;
	double t;

	uint8_t *buf = builder.GetBufferPointer();
	int size = builder.GetSize();
	std::cout<<size<<std::endl;

	gettimeofday(&start, NULL);
	auto item = GetLINEITEM(buf);
	gettimeofday(&end, NULL);

	assert(strcmp(item->L_SHIPINSTRUCT()->c_str(),"Use lots of tape")==0);
	
	t = (double) ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));

	std::cout<<"Reading LINEITEM took "<< t<< " microseconds"<<std::endl;

	return 0;
}

