#!/usr/bin/env python

from hadoop.io.Writable import AbstractValueWritable
from hadoop.io.OutputStream import DataOutputBuffer
# try and load the fastest typedbytes
try:
	import ctypedbytes as typedbytes
except ImportError:
    try:
        import typedbytes
    except ImportError:
        from hadoop.typedbytes import typedbytes

class TypedBytesWritable(AbstractValueWritable):
    def write(self, data_output):
        tmpout = DataOutputBuffer()
        output = typedbytes.Output(tmpout)
        output.write(self._value)
        bytes_len = tmpout.getSize()
        #print 'first 4 bytes in data output is ',bytes_len
        data_output.writeInt(bytes_len)
        data_output.write(tmpout.toByteArray())
        
    def readFields(self, data_input):
        # print "in read fields!"
        bytes_len = data_input.readInt()
        #print 'first 4 bytes in data input is ',bytes_len
        input = typedbytes.Input(data_input)
        for record in input:
            self._value = record