#!/bin/python
import os
for i in range( 0, 10000 ) :
  os.system( "rados -p paper_exps rm obj." + str(i) )
  os.system( "rados -p paper_exps rm obj.mergetarget." + str(i) )
  os.system( "rados -p paper_exps rm obj.client.mergetarget." + str(i) )
