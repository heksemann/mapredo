mapredo
=======

*Mapreducing at the speed of C*

### Overview

Mapredo is C++11 based software inspired by Google's famous MapReduce paper.  The main objectives are:

- Speed
- Ease of use
- Flexibility

Some of the limitations are:

- Currently only a single computer/server is supported

### Mini-HOWTO

    wget http://www.gutenberg.org/cache/epub/100/pg100.txt
    cat pg100.txt | mapredo wordcount
    cat pg100.txt | mapredo wordcount | mapredo wordsort
    


