#!/usr/bin/python

import sys,getopt,os,io
import struct,time,math
import calendar
from datetime import datetime, date

def usage(): 
    print (
        'Converts TPCH dbgen delimited data for any table into mutiple files' 
        ' of binary data for Ceph objects. Each file contains <rows> of data.'
        ' By default it assumes the schema of the'
        ' lineitem table. Schema is denoted below as char.1 for char(1).'
        ' ints are represented as 4 bytes.'
        ' date is converted to utc and written as int. '
        ' chars and varchars are truncated to their specified bytes. ' 
        ' decimal(15,2) is taken as a 15 character string and converted '
        ' to an 8 byte float, can be denoted as decimal.15 for decimal(15,2). \n '
        )
        
    print (
        'Usage: \n'
        'convert_tpch-tables-v1.py [args]\n' 
        ' non-parameterized args: \n' 
        ' -h[--help] prints this menu \n'   
        ' -d[--debug] (default=no)\n'
        ' -a[--align] use native byte alignment (default=no)\n'   
        ' \n'
        ' parameterized args: \n'            
        ' -i[--input] <inputfile> (default=none)\n' 
        ' -r[--rows]  <rows_per_object> (default=1)\n'
        ' -m[--delim] <delimiter> (default=\'|\') \n'
        ' -s[--schema] <schema, ie. col types and widths> '
        '(default=int,int,int,int,decimal.15,decimal.15,decimal.15'
        'decimal.15,char.1,char.1,date,date,date,char.25,char.10,'
        'varchar.44)\n'
        )
        
    print(
        'Example:\n'
        'python convert-tpch-tables.py -i lineitem-1K-rows.tbl -r 10 \n'
        'python convert-tpch-tables.py -i some-table-csv.tbl -r 5000 -m \|'
        ' -s int,int,varchar.40,char.10,char.1,decimal.15,date -d\n'
        )
        
"""
TPCH lineitemt table schema and this converter's binary conversion of each col
CREATE TABLE LINEITEM (
                             L_ORDERKEY    INTEGER NOT NULL,  as 4 byte int
                             L_PARTKEY     INTEGER NOT NULL,  4 byte int
                             L_SUPPKEY     INTEGER NOT NULL,  4 byte int
                             L_LINENUMBER  INTEGER NOT NULL,  4 byte int
                             L_QUANTITY    DECIMAL(15,2) NOT NULL, 8 byte double
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, 8 byte double
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL, 8 byte double
                             L_TAX         DECIMAL(15,2) NOT NULL, 8 byte double
                             L_RETURNFLAG  CHAR(1) NOT NULL, 1 byte char
                             L_LINESTATUS  CHAR(1) NOT NULL, 1 byte char
                             L_SHIPDATE    DATE NOT NULL, 4 byte int (utc)
                             L_COMMITDATE  DATE NOT NULL, 4 byte int (utc)
                             L_RECEIPTDATE DATE NOT NULL, 4 byte int (utc)
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,  25 byte char
                             L_SHIPMODE     CHAR(10) NOT NULL, 10 byte char
                             L_COMMENT      VARCHAR(44) NOT NULL);  44 byte char
"""

# write out and close the file, report size
def flushFile(outfile):
    if outfile is not None:
        if not outfile.closed:
            outfile.flush()
            os.fsync(outfile.fileno())
            outfile.close()
            size = os.stat(outfile.name).st_size
            print ('Wrote file {:}; size is {:d} Bytes; {:.2f} MB'
                .format(outfile.name, size, size/float(1024*1024)))
        

def main(argv):
    infile = ''
    
    # defaults to the TPCH lineitem table as noted above in comments. 
    schema = [
        "int","int","int","int","decimal.15","decimal.15","decimal.15",
        "decimal.15","char.1","char.1","date","date","date","char.25",
        "char.10","varchar.44"
    ]
    
    # default param values
    alignment = False # no byte alignment
    delim = '|'
    debug = False
    rows_per_object = 1
    
    try:
        opts, args = getopt.getopt(
            argv,
            "hbtadi:o:s:m:r:",
            ["input=","schema=","delim=","debug=","help=","rows=" ])
            
    except getopt.GetoptError:
        usage()
        sys.exit(2)
        
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-d", "--debug"):
            debug = True
        elif opt in ("-i", "--input"):
            infile = arg
        elif opt in ("-s", "--schema"):
            schema = arg.split(',')     
        elif opt in ("-a", "--align"):
            alignment = True
        elif opt in ("-m", "--delim"):
            delim = arg
            assert len(delim) == 1, "delimiter must be a single character"
        elif opt in ("-r", "--rows"):
            rows_per_object = int(arg)
            
    assert len(infile) > 0, "An input file name must be provided."
    print 'Input file is ', infile
    print 'cols=>', ",".join(schema)
    
    # compute the input file size
    size = float(os.stat(infile).st_size) / float(1024*1024)
    print ('input file size is {:d} {:}; {:.2f} {:}'
        .format(os.stat(infile).st_size, 'Bytes', size, 'MB'))   

    # main loop:
    # for each line. we create an array of values, decoding them based on type
    # and adding to an array of vals that is used to create a struct for that 
    # line we write out the struct of vals, and its schema (format_string).
    outfile = None
    line_number = 0
    object_number = 0
    object_row_counter = 0
    
    with open(infile) as f:
        for line in f:

            if debug:
                print "line_number=", line_number
                print "object_row_counter=", object_row_counter

            # create a new binary file to write our binary data.
            if outfile is None:
                print "\nCreating a new file number", object_number
                outfile = io.open(
                    ("obj" + str(object_number).zfill(7) + ".bin"), 'wb')
                object_number += 1
                object_row_counter = 0
            
            # update input and output file counters
            line_number += 1
            object_row_counter += 1
                
            # split the line on the delim, assert row is complete according to 
            # schema provided.
            fields= line.split(delim)
            data = [f.strip() for f in fields]
            row = data[0:len(data)-1] # ignore the last split (it is empty)
            assert len(row) == len(schema), ( 
                ' line_number {:d} has {:d} cols, expected {:d} cols,'
                ' using delim=\'{:}\' line={:}\nbut parsed cols={:}'
                .format(line_number, len(row), len(schema), delim, line, row))

            # start the format_string for this struct, which is appended to for each struct val 
            # and used for en/decode
            format_string = ""
            if alignment:
                format_string += "@"  # native byte alignment
            else:
                format_string += "="  # no byte alignment
            vals_list = []

            format_string += "iiiidddd1s1siii25s10s44s"
            
            #"int",
            #format_string += "i"
            vals_list.append(int(row[0]))
            
            #"int",
            #format_string += "i"
            vals_list.append(int(row[1]))
            
            #"int",
            #format_string += "i"
            vals_list.append(int(row[2]))
            
            #"int",
            #format_string += "i"
            vals_list.append(int(row[3]))
            
            # "decimal.15"
            #format_string += "d"
            vals_list.append(float(row[4]))
            
            # "decimal.15"
            #format_string += "d"
            vals_list.append(float(row[5]))
            
            # "decimal.15"
            #format_string += "d"
            vals_list.append(float(row[6]))
            
            # "decimal.15"
            #format_string += "d"
            vals_list.append(float(row[7]))
            
            #"char.1"
            #format_string += ("1" + "s")           # auto-pads to n inside struct
            vals_list.append(row[8])
  
            #"char.1"
            #format_string += ("1" + "s")           # auto-pads to n inside struct
            vals_list.append(row[9])
            
            #"date"
            #format_string += "i" 
            dt = datetime.strptime(row[10], "%Y-%m-%d")
            vals_list.append(calendar.timegm(dt.utctimetuple()))  # returns utc
                     
            #"date"
            #format_string += "i" 
            dt = datetime.strptime(row[11], "%Y-%m-%d")
            vals_list.append(calendar.timegm(dt.utctimetuple()))  # returns utc
            
            #"date"
            #format_string += "i" 
            dt = datetime.strptime(row[12], "%Y-%m-%d")
            vals_list.append(calendar.timegm(dt.utctimetuple()))  # returns utc            
            
            # "char.25"
            #format_string += ("25" + "s")           # auto-pads to n inside struct
            vals_list.append(row[13])
    
            # "char.10"
            #format_string += ("10" + "s")           # auto-pads to n inside struct
            vals_list.append(row[14])
            
            # "varchar.44"
            #format_string += ("44" + "s")           # auto-pads to n inside struct
            vals_list.append(row[15])            

            if debug:
                print ("writing " + outfile.name +"; object_row_counter=" 
                    + str(object_row_counter))
                
            # setup to write the data to binary file via struct bytes
            struct_args_list = []
            struct_args_list.append(format_string)
            struct_args_list.extend(vals_list)
            
            # see: https://docs.python.org/2/library/struct.html
            # example use: struct.pack(<format_string>,<list of data vals>)
            # data = struct.pack('=iii2siq', 1123456,2,3,"ss", 3, 4)
            # print 'data unpacked=', struct.unpack('=iii2siq', data)
            
            # write out the row as binary struct
            data = struct.pack(*struct_args_list)
            outfile.write(data)
            
            if debug:
                print ('line#' + str(line_number) + 
                    '; struct data unpacked=', 
                    struct.unpack(format_string, data))
            
            # write out the object file.
            if line_number % rows_per_object == 0:
                #flushFile(outfile)
                outfile = None
     
    # write out any reamining rows, we are finished with the input.
    flushFile(outfile)
    
    print '\nSummary:'
    print 'total lines of input file processed =', line_number    
    print 'requested rows per object =', rows_per_object   
    print 'total objects created =', object_number
    
    
if __name__ == "__main__":
    main(sys.argv[1:])
        