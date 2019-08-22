import random
import string
import sys
import os
import numpy as np

def dsgen( outfile_name, arity, numrows, splitter ) :

  os.system( 'rm ' + outfile_name )
  outfile = open( outfile_name, 'a' )

  # arity3 (int,float,str) ----------------------------- #
  if arity == 3 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"
    counter = 0
    for k in range( 0, numrows / splitter ) :
      for i in range( 0, splitter ) :
        random.seed( counter+0 )
        anint   = np.uint32( random.randint( 0, 100 ) )
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
    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"
    counter = 0
    for k in range( 0, numrows / splitter ) :
      for i in range( 0, splitter ) :
        random.seed( counter+0 )
        anint0  = np.uint32( random.randint( 0, 100 ) )
        random.seed( counter+1 )
        anint1  = np.uint32( random.randint( 0, 100 ) )
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
    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"
    counter = 0
    for k in range( 0, numrows / splitter ) :
      for i in range( 0, splitter ) :
        arow = ""
        first = True
        for j in range( 0, arity ) :
          if( first == True ) :
            first = False
          else :
            arow = arow + "|"
          random.seed( counter+1 )
          arow = arow + str( np.uint32( random.randint( 10000000, 99999999 ) ) )

        counter += 1
        outfile.write( arow + "\n" )
        if( counter % 1000 == 0 ) :
          print outfile_name + " : writing row #" + str( counter )

  # wut? ----------------------------- #
  else :
    print "artiy '" + str( arity ) + "' not supported."

  outfile.close()

def main() :

  numrows      = sys.argv[1]
  arity        = sys.argv[2]
  obj_id       = sys.argv[3]
  splitter     = sys.argv[4]
  outfile_name = 'dataset_arity' + arity + '_' + str( numrows ) + '_rows-' + obj_id + '.txt'
  print "obj_id = " + obj_id

  dsgen( outfile_name, int( arity ), int( numrows ), int( splitter ) )

if __name__ == "__main__" :
  main()
