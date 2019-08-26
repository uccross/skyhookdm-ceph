import multiprocessing
import os
import sys

#####################
#   GEN ARITY 100   #
#####################
def gen_arity100( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir ) :

  schema_datatypes  = ["int"]*100
  schema_iskey      = ["0"]*100
  schema_isnullable = ["0"]*100

  schema_attnames = []
  for i in range(0,100) :
    schema_attnames.append( 'att' + str(i) )

  schema_datatypes_str  = ','.join( schema_datatypes )
  schema_iskey_str      = ','.join( schema_iskey )
  schema_isnullable_str = ','.join( schema_isnullable )
  schema_attnames_str   = ','.join( schema_attnames )

  command = 'bin/fbwriter_fbu \
    --filename ' + f + ' \
    --write_type ' + orientation + ' \
    --debug no \
    --schema_datatypes ' + schema_datatypes_str + ' \
    --schema_attnames ' + schema_attnames_str + ' \
    --table_name atable \
    --nrows ' + str( nrows ) + ' \
    --ncols 100\
    --targetoid ' + testid + ' \
    --targetpool tpchflatbuf \
    --targetformat ' + targetformat + ' \
    --writeto disk \
    --cols_per_fb ' + str( cols_per_fb ) + ' \
    --schema_iskey ' + schema_iskey_str + ' \
    --schema_isnullable ' + schema_isnullable_str + ' \
    --savedir ' + savedir + ' \
    --rid_start_value 1 \
    --rid_end_value ' + str( nrows ) + ' \
    --obj_counter ' + str( obj_counter ) + ' ;'
  print command
  os.system( command )

###################
#   GEN ARITY 4   #
###################
def gen_arity4( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir ) :

  command = 'bin/fbwriter_fbu \
    --filename ' + f + ' \
    --write_type ' + orientation + ' \
    --debug no \
    --schema_datatypes int,int,float,string \
    --schema_attnames att0,att1,att2,att3 \
    --table_name atable \
    --nrows ' + str( nrows ) + ' \
    --ncols 4\
    --targetoid ' + testid + ' \
    --targetpool tpchflatbuf \
    --targetformat ' + targetformat + ' \
    --writeto disk \
    --cols_per_fb ' + str( cols_per_fb ) + ' \
    --schema_iskey 0,0,0,0 \
    --schema_isnullable 0,0,0,0 \
    --savedir ' + savedir + ' \
    --rid_start_value 1 \
    --rid_end_value ' + str( nrows ) + ' \
    --obj_counter ' + str( obj_counter ) + ' ;'
  print command
  os.system( command )

###################
#   GEN ARITY 3   #
###################
def gen_arity3( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir ) :

  command = 'bin/fbwriter_fbu \
    --filename ' + f + ' \
    --write_type ' + orientation + ' \
    --debug no \
    --schema_datatypes int,float,string \
    --schema_attnames att0,att1,att2 \
    --table_name atable \
    --nrows ' + str( nrows ) + ' \
    --ncols 3 \
    --targetoid ' + testid + ' \
    --targetpool tpchflatbuf \
    --targetformat ' + targetformat + ' \
    --writeto disk \
    --cols_per_fb ' + str( cols_per_fb ) + ' \
    --schema_iskey 0,0,0 \
    --schema_isnullable 0,0,0 \
    --savedir ' + savedir + ' \
    --rid_start_value 1 \
    --rid_end_value ' + str( nrows ) + ' \
    --obj_counter ' + str( obj_counter ) + ' ;'
  print command
  os.system( command )

################
#   ROWS GEN   #
################
def rows_gen( arity, testid, f, nrows, obj_counter, cols_per_fb, savedir ) :

  orientation  = "rows"
  targetformat = "SFT_FLATBUF_UNION_ROW"
  cols_per_fb  = 4

  if arity == 3 :
    gen_arity3( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir )

  elif arity == 4 :
    gen_arity4( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir )

  elif arity == 100 :
    gen_arity100( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir )

  else :
    print "not supported: arity = " + str( arity )

################
#   COLS GEN   #
################
def cols_gen( arity, testid, f, nrows, obj_counter, cols_per_fb, savedir ) :

  orientation  = "cols"
  targetformat = "SFT_FLATBUF_UNION_COL"

  if arity == 3 :
    gen_arity3( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir )

  elif arity == 4 :
    gen_arity4( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir )

  elif arity == 100 :
    gen_arity100( testid, f, nrows, obj_counter, orientation, targetformat, cols_per_fb, savedir )

  else :
    print "not supported: arity = " + str( arity )

#################
#   GET FILES   #
#################
# get filenames in directory
def get_files( path, prefix ) :
  files = []
  # r=root, d=directories, f = files
  for r, d, f in os.walk(path):
    for filename in f:
      if filename.startswith( prefix ) :
        files.append(os.path.join(r, filename))
  print "len(files) = " + str( len(files) )
  return files


##############
#   DRIVER   #
##############
def driver( 
  orientation,
  nrows,
  arity,
  testid,
  prefix,
  path,
  cols_per_fb ) :

  print "orientation = " + orientation
  print "nrows       = " + str( nrows )
  print "arity       = " + str( arity )
  print "testid      = " + testid
  print "prefix      = " + prefix
  print "path        = " + path
  print "cols_per_fb = " + str( cols_per_fb )

  files = get_files( path, prefix )  
  savedir = os.path.abspath( "/mnt/storage4/pdsw19/" + testid + "/" )
  print "savedir     = " + str( savedir )

  # check if dir exists. if not make it.
  if os.path.isdir( savedir ) :
    print "save dir path exists: " + savedir
  else :
    os.system( "mkdir -p " + savedir )

  obj_counter = 0
  for f in files:
    print(f)

    if orientation == "rows" :
      rows_gen( arity, testid, f, nrows, obj_counter, cols_per_fb, savedir )
    elif orientation == "cols" :
      cols_gen( arity, testid, f, nrows, obj_counter, cols_per_fb, savedir )
    else :
      print "not supported orientation = '" + orientation + "'"

    #if obj_counter == 0 :
    #  break
    obj_counter += 1

############
#   MAIN   #
############
def main() :
#  try :
#    test = sys.argv[1]
#  except IndexError as e :
#    print str(e) + "\n   please supply test name e.g. python fbwriter_fbu_gen.py arity3-1mb-100objs"
#    sys.exit(1) ;


  job_args_list = [

    # ------------------------------------------ #
    # 100objs

   # ROWS : arity3-1mb-100objs
   ( "rows", 50000, 3, "fburows-arity3-1mb-100objs", \
     "dataset_arity3_50000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 0 ),
#   # ROWS : arity3-10mb-100objs
#    ( "rows", 500000, 3, "fburows-arity3-10mb-100objs", \
#      "dataset_arity3_500000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 0 ),
#    # ROWS : arity3-100mb-100objs
#    ( "rows", 5000000, 3, "fburows-arity3-100mb-100objs", \
#      "dataset_arity3_5000000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 0 ),
#
#    # COLS 4 : arity3-1mb-100objs
#    ( "cols", 50000, 3, "fbucols4-arity3-1mb-100objs", 
#      "dataset_arity3_50000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 4 ),
#    # COLS 4 : arity3-10mb-100objs
#    ( "cols", 500000, 3, "fbucols4-arity3-10mb-100objs", 
#      "dataset_arity3_500000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 4 ),
#    # COLS 4 : arity3-100mb-100objs
#    ( "cols", 5000000, 3, "fbucols4-arity3-100mb-100objs", 
#      "dataset_arity3_5000000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 4 ),
#
#    # ROWS : arity100-1mb-100objs
#    ( "rows", 2500, 100, "fburows-arity100-1mb-100objs", 
#      "dataset_arity100_2500_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 0 ),
#    # ROWS : arity100-10mb-100objs
#    ( "rows", 25000, 100, "fburows-arity100-10mb-100objs", 
#      "dataset_arity100_25000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 0 ),
#    # ROWS : arity100-100mb-100objs
#    ( "rows", 250000, 100, "fburows-arity100-100mb-100objs", 
#      "dataset_arity100_250000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 0 ),
#
#    # COLS 100 : arity100-1mb-100objs
#    ( "cols", 2500, 100, "fbucols100-arity100-1mb-100objs", 
#      "dataset_arity100_2500_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 100 ),
#    # COLS 100 : arity100-10mb-100objs
#    ( "cols", 25000, 100, "fbucols100-arity100-10mb-100objs", 
#      "dataset_arity100_25000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 100 ),
#    # COLS 100 : arity100-100mb-100objs
#    ( "cols", 250000, 100, "fbucols100-arity100-100mb-100objs", 
#      "dataset_arity100_250000_rows-", "/mnt/storage1/pdsw19/raw_csv_100objs/", 100 ),
#
#    # ------------------------------------------ #
#    # 1000objs
#
#    # ROWS : arity3-1mb-1000objs
#    ( "rows", 50000, 3, "fburows-arity3-1mb-1000objs", \
#      "dataset_arity3_50000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 0 ),
#    # ROWS : arity3-10mb-1000objs
#    ( "rows", 500000, 3, "fburows-arity3-10mb-1000objs", \
#      "dataset_arity3_500000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 0 ),
#    # ROWS : arity3-100mb-1000objs
#    ( "rows", 5000000, 3, "fburows-arity3-100mb-1000objs", \
#      "dataset_arity3_5000000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 0 ),
#
#    # COLS 4 : arity3-1mb-1000objs
#    ( "cols", 50000, 3, "fbucols4-arity3-1mb-1000objs", 
#      "dataset_arity3_50000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 4 ),
#    # COLS 4 : arity3-10mb-1000objs
#    ( "cols", 500000, 3, "fbucols4-arity3-10mb-1000objs", 
#      "dataset_arity3_500000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 4 ),
#    # COLS 4 : arity3-100mb-1000objs
#    ( "cols", 5000000, 3, "fbucols4-arity3-100mb-1000objs", 
#      "dataset_arity3_5000000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 4 ),
#
#    # ROWS : arity100-1mb-1000objs
#    ( "rows", 2500, 100, "fburows-arity100-1mb-1000objs", 
#      "dataset_arity100_2500_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 0 ),
#    # ROWS : arity100-10mb-1000objs
#    ( "rows", 25000, 100, "fburows-arity100-10mb-1000objs", 
#      "dataset_arity100_25000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 0 ),
#    # ROWS : arity100-100mb-1000objs
#    ( "rows", 250000, 100, "fburows-arity100-100mb-1000objs", 
#      "dataset_arity100_250000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 0 ),
#
#    # COLS 100 : arity100-1mb-1000objs
#    ( "cols", 2500, 100, "fbucols100-arity100-1mb-1000objs", 
#      "dataset_arity100_2500_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 100 ),
#    # COLS 100 : arity100-10mb-1000objs
#    ( "cols", 25000, 100, "fbucols100-arity100-10mb-1000objs", 
#      "dataset_arity100_25000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 100 ),
#    # COLS 100 : arity100-100mb-1000objs
#    ( "cols", 250000, 100, "fbucols100-arity100-100mb-1000objs", 
#      "dataset_arity100_250000_rows-", "/mnt/storage1/pdsw19/raw_csv_1000objs/", 100 ),
  ]

  # printing main program process id
  print("ID of main process: {}".format(os.getpid()))
  jobs = []
  for i in range( 0, len( job_args_list ) ) :
    job_args = job_args_list[i]
    print "======================================================"
    print job_args
    print "======================================================"
    job_name = job_args[3]
    print job_name
    j = multiprocessing.Process( target=driver, args=job_args )
    jobs.append( j )
    j.start()
    print( "ID of process : {}".format( j.pid ) )

  # wait until processes finish
  for j in jobs :
    j.join()

  # all processes finished
  print("all processes finished execution!")

'''
# ------------------------------------------ #
# ARITY 3

  if test == "fburows-arity3-1mb-100objs" :
    main( "rows", 
          50000, 
          3, 
          "fburows-arity3-1mb-100objs", 
          "dataset_arity3_50000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          0 )

  # ROWS : arity3-10mb-100objs
  elif test == "fburows-arity3-10mb-100objs" :
    main( "rows", 
          500000, 
          3, 
          "fburows-arity3-10mb-100objs", 
          "dataset_arity3_500000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          0 )

  # ROWS : arity3-100mb-100objs
  elif test == "fburows-arity3-100mb-100objs" :
    main( "rows", 
          5000000, 
          3, 
          "fburows-arity3-100mb-100objs", 
          "dataset_arity3_5000000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          0 )

  # COLS 4 : arity3-1mb-100objs
  elif test == "fbucols4-arity3-1mb-100objs" :
    main( "cols", 
          50000, 
          3, 
          "fbucols4-arity3-1mb-100objs", 
          "dataset_arity3_50000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          4 )

  # COLS 4 : arity3-10mb-100objs
  elif test == "fbucols4-arity3-10mb-100objs" :
    main( "cols", 
          500000, 
          3, 
          "fbucols4-arity3-10mb-100objs", 
          "dataset_arity3_500000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          4 )

  # COLS 4 : arity3-100mb-100objs
  elif test == "fbucols4-arity3-100mb-100objs" :
    main( "cols", 
          5000000, 
          3, 
          "fbucols4-arity3-100mb-100objs", 
          "dataset_arity3_5000000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          4 )

# ------------------------------------------ #
# ARITY 4

  # COLS 4 : arity4-1mb-100objs
  elif test == "fbucols4-arity4-1mb-100objs" :
    main( "cols", 
          42000, 
          4, 
          "fbucols4-arity4-1mb-100objs", 
          "dataset_arity4_42000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          4 )

  # COLS 4 : arity4-10mb-100objs
  elif test == "fbucols4-arity4-10mb-100objs" :
    main( "cols", 
          420000, 
          4, 
          "fbucols4-arity4-10mb-100objs", 
          "dataset_arity4_420000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          4 )

  # COLS 4 : arity4-100mb-100objs
  elif test == "fbucols4-arity4-100mb-100objs" :
    main( "cols", 
          4200000, 
          4, 
          "fbucols4-arity4-100mb-100objs", 
          "dataset_arity4_4200000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          4 )

  # COLS 2 : arity4-1mb-100objs
  elif test == "fbucols2-arity4-1mb-100objs" :
    main( "cols", 
          42000, 
          4, 
          "fbucols2-arity4-1mb-100objs", 
          "dataset_arity4_42000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          2 )

  # COLS 2 : arity4-10mb-100objs
  elif test == "fbucols2-arity4-10mb-100objs" :
    main( "cols", 
          420000, 
          4, 
          "fbucols2-arity4-10mb-100objs", 
          "dataset_arity4_420000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          2 )

  # COLS 2 : arity4-100mb-100objs
  elif test == "fbucols2-arity4-100mb-100objs" :
    main( "cols", 
          4200000, 
          4, 
          "fbucols2-arity4-100mb-100objs", 
          "dataset_arity4_4200000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          2 )

  # COLS 1 : arity4-1mb-100objs
  elif test == "fbucols1-arity4-1mb-100objs" :
    main( "cols", 
          42000, 
          4, 
          "fbucols1-arity4-1mb-100objs", 
          "dataset_arity4_42000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          1 )

  # COLS 1 : arity4-10mb-100objs
  elif test == "fbucols1-arity4-10mb-100objs" :
    main( "cols", 
          420000, 
          4, 
          "fbucols1-arity4-10mb-100objs", 
          "dataset_arity4_420000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          1 )

  # COLS 1 : arity4-100mb-100objs
  elif test == "fbucols1-arity4-100mb-100objs" :
    main( "cols", 
          4200000, 
          4, 
          "fbucols1-arity4-100mb-100objs", 
          "dataset_arity4_4200000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          1 )

# ------------------------------------------ #
# ARITY 100

  # ROWS : arity100-1mb-100objs
  elif test == "fburows-arity100-1mb-100objs" :
    main( "rows", 
          2500, 
          100, 
          "fburows-arity100-1mb-100objs", 
          "dataset_arity100_2500_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          0 )

  # ROWS : arity100-10mb-100objs
  elif test == "fburows-arity100-10mb-100objs" :
    main( "rows", 
          25000, 
          100, 
          "fburows-arity100-10mb-100objs", 
          "dataset_arity100_25000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          0 )

  # ROWS : arity100-100mb-100objs
  elif test == "fburows-arity100-100mb-100objs" :
    main( "rows", 
          250000, 
          100, 
          "fburows-arity100-100mb-100objs", 
          "dataset_arity100_250000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          0 )

  # COLS 100 : arity100-1mb-100objs
  elif test == "fbucols100-arity100-1mb-100objs" :
    main( "cols", 
          2500, 
          100, 
          "fbucols100-arity100-1mb-100objs", 
          "dataset_arity100_2500_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          100 )

  # COLS 100 : arity100-10mb-100objs
  elif test == "fbucols100-arity100-10mb-100objs" :
    main( "cols", 
          25000, 
          100, 
          "fbucols100-arity100-10mb-100objs", 
          "dataset_arity100_25000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          100 )

  # COLS 100 : arity100-100mb-100objs
  elif test == "fbucols100-arity100-100mb-100objs" :
    main( "cols", 
          250000, 
          100, 
          "fbucols100-arity100-100mb-100objs", 
          "dataset_arity100_250000_rows-", 
          "/mnt/storage1/pdsw19/raw_csv_100objs/", 
          100 )
'''

####################################
#     MAIN THREAD OF EXECUTION     #
####################################
if __name__ == "__main__" :
  main()
