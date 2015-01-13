mapredo
=======

*Mapreducing at the speed of C*

### Overview

Mapredo is C++11 based software inspired by Google's famous MapReduce paper, with focus on small to medium sized data.  Some of the features are:

- Speedy, does word count of the collected works of Shakespeare in <.30s on a 2010 i5 gen 1 laptop
- Easy to use, can be pipelined and used with command line tools
- Compression support (snappy)
- Supports modern Linux distros (gcc) and Windows (Visual Studio)
- Does not require any configuration, just install and run

Some of the current limitations are:

- Only a single computer/server can be utilized
- Can not be natively embedded into other processing frameworks, but pipes may be used
- Not tested on other UNIX like systems, including OS X

### Mini-HOWTO

    wget http://www.gutenberg.org/cache/epub/100/pg100.txt
    cat pg100.txt | mapredo wordcount
    cat pg100.txt | mapredo wordcount | mapredo wordsort
    


