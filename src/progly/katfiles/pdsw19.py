import multiprocessing
import numpy as np
import os
import random
import string
import sys

#############
#   DSGEN   #
#############
def dsgen( obj_id, \
           numobjs, \
           numrows, \
           arity, \
           outfile_path, \
           outfile_name ) :

  print "--------------------------------------"
  print "obj_id       = " + str( obj_id )
  print "numobjs      = " + str( numobjs )
  print "numrows      = " + str( numrows )
  print "arity        = " + str( arity )
  print "outfile_path = " + outfile_path
  print "outfile_name = " + outfile_name

  outfile_n = outfile_path + "/" + outfile_name
  os.system( 'rm ' + outfile_n )
  outfile = open( outfile_n, 'a' )

  start_row = numrows*obj_id
  end_row   = start_row + numrows - 1

  print "start_row = " + str( start_row )
  print "end_row   = " + str( end_row )

  # arity1 (int) ----------------------------- #
  if arity == 1 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_n + "'"
    counter = 0
    for i in range( start_row, end_row+1 ) :
      random.seed( counter+0 )
      anint   = np.uint32( random.randint( 1000, 9999 ) )

      arow = str( anint )

      counter += 1
      outfile.write( arow + "\n" )
      if( counter % 1000 == 0 ) :
        print outfile_name + " : writing row #" + str( counter )

  # arity3 (int,float,str) ----------------------------- #
  elif arity == 3 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_n + "'"
    counter = 0
    for i in range( start_row, end_row+1 ) :
      random.seed( counter+0 )
      anint   = np.uint32( random.randint( 0, 1000 ) )
      random.seed( counter+1 )
      afloat  = np.float32( random.random() )
      random.seed( counter+2 )
      basestr = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( 10 ) ] )

      random.seed( counter+3 )
      extra_digits = random.randint( 0, 4 )
      random.seed( counter+4 )
      astr1  = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( extra_digits ) ] )
      astr   = basestr+astr1

      arow = str( anint ) + "|" + str( afloat ) + "|" + astr

      counter += 1
      outfile.write( arow + "\n" )
      if( counter % 1000 == 0 ) :
        print outfile_name + " : writing row #" + str( counter )

  # arity4 (int,int,float,str) ----------------------------- #
  elif arity == 4 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_n + "'"
    counter = 0
    for i in range( start_row, end_row+1 ) :
      random.seed( counter+0 )
      anint0  = np.uint32( random.randint( 0, 1000 ) )
      random.seed( counter+1 )
      anint1  = np.uint32( random.randint( 0, 1000 ) )
      random.seed( counter+2 )
      afloat  = np.float32( random.random() )
      random.seed( counter+3 )
      basestr = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( 10 ) ] )

      random.seed( counter+4 )
      extra_digits = random.randint( 0, 4 )
      random.seed( counter+5 )
      astr1  = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( extra_digits ) ] )
      astr   = basestr+astr1

      arow = str( anint0 ) + "|" + str( anint1 ) + "|" + str( afloat ) + "|" + astr

      counter += 1
      outfile.write( arow + "\n" )
      if( counter % 1000 == 0 ) :
        print outfile_name + " : writing row #" + str( counter )

  # arity100 (int,int,...,int) ----------------------------- #
  elif arity == 100 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_n + "'"
    counter = 0
    for i in range( start_row, end_row+1 ) :
      arow = ""
      first = True
      for j in range( 0, arity ) :
        if( first == True ) :
          first = False
        else :
          arow = arow + "|"
        random.seed( counter+i+j+1 )
        arow = arow + str( np.uint32( random.randint( 0, 10000 ) ) )

      counter += 1
      outfile.write( arow + "\n" )
      if( counter % 1000 == 0 ) :
        print outfile_name + " : writing row #" + str( counter )

  # wut? ----------------------------- #
  else :
    print "artiy '" + str( arity ) + "' not supported."

  outfile.close()


############
#   MAIN   #
############
def main() :

  # inputs
  numobjs      = int( sys.argv[1] ) # total num files to create
  numrows      = int( sys.argv[2] ) # number of rows per object
  arity        = int( sys.argv[3] ) # number of columns in tables
  outfile_path = sys.argv[4]        # directory for saving outfiles


  # printing main program process id 
  print("ID of main process: {}".format(os.getpid()))

  jobs = []
  for i in range( 0, numobjs ) :

    obj_id = i
    job_name     = "job" + str( obj_id )
    outfile_name = 'dataset_arity' + str( arity ) + \
                   '_' + str( numrows ) + '_rows-' + str( obj_id ) + '.txt'

    # job arguments
    job_args = ( obj_id, \
                 numobjs, \
                 numrows, \
                 arity, \
                 outfile_path, \
                 outfile_name )

    j = multiprocessing.Process( target=dsgen, \
                                 name=job_name, \
                                 args=job_args )
    jobs.append( j )
    j.start()
    print( "ID of process " + job_name + " : {}".format( j.pid ) ) 

  # wait until processes finish
  for j in jobs :
    j.join() 
  
  # all processes finished 
  print("all processes finished execution!") 


###########################
#   THREAD OF EXECUTION   #
###########################
if __name__ == "__main__" :
  main()
