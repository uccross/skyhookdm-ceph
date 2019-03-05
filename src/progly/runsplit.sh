bin/rados -p tpchflatbuf rm split1
bin/rados -p tpchflatbuf rm dst0
bin/rados -p tpchflatbuf rm dst1
bin/rados -p tpchflatbuf rm split1_split10
bin/rados -p tpchflatbuf rm split1_split11
bin/rados -p tpchflatbuf rm split1_split20
bin/rados -p tpchflatbuf rm split1_split21
bin/run-split
bin/rados -p tpchflatbuf ls
