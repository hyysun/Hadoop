import os
import unittest
import decimal
import datetime

import typedbytes as rawtb
import hadoop.typedbytes as tb
import hadoop.io.OutputStream as out
import hadoop.io.InputStream as input

class MyClass:
	def __init__(self, val):
		self.val = val		

class TestTypedBytesWritable(unittest.TestCase):
    objects = [True, 1234, 3000000000, 12345L, 1.23, "trala", u'trala',
                (1,2,3), [1,2,3,4], {1:2,3:4}, set([1,2,3]),
                decimal.Decimal("123.456"), datetime.datetime.now(),
                rawtb.Bytes("abc123\x01")]

    def setUp(self):
      	# may want stuff here eventually 
      	pass
      
    def test_setget(self):
    	t = tb.TypedBytesWritable()
    	t.set(-1)
    	self.assertEqual(t.get(), -1)
    	
    def test_outputlength(self):
    	t = tb.TypedBytesWritable()
    	val = {'hi':'there'}
    	t.set(val)
    	buf1 = out.DataOutputBuffer()
    	t.write(buf1)
    	buf2 = out.DataOutputBuffer()
    	rtb = rawtb.Output(buf2)
    	rtb.write(val)
    	# buf2 should have the length appended for a slightly longer string
    	self.assertEqual(buf1.getSize(), buf2.getSize() + 4)	
  
    def read_buffer(self, buf):
      	bytes_len = buf.readInt()
        input = rawtb.Input(buf)
        nrecords = 0
        for record in input:
            value = record
            nrecords += 1
        self.assertEqual(nrecords, 1)
        return value
        
    def do_input_output(self,val):
    	# write val to a buffer
    	t = tb.TypedBytesWritable()
    	t.set(val)
    	buf1 = out.DataOutputBuffer()
    	t.write(buf1)
    	# read val from a buffer
    	buf2 = input.DataInputBuffer(buf1.toByteArray())
    	value = self.read_buffer(buf2)
    	# make sure they are the same!
    	self.assertEqual(val,value)
    	
        
    def test_types(self):
        # make sure output AND input works for
        # arrays, maps, strings, doubles, ints, 
        # and all basic types
        objects = TestTypedBytesWritable.objects
        for record in objects:
            self.do_input_output(record) 
    	
    def test_pickle(self):
     	# TypedBytes know how to output MyClass
     	badoutput = MyClass(5)
    	t = tb.TypedBytesWritable()
    	t.set(badoutput) # this is okay...
        buf1 = out.DataOutputBuffer()
    	self.assertRaises(t.write(buf1)) # this will work.
    	buf2 = input.DataInputBuffer(buf1.toByteArray())
    	value = self.read_buffer(buf2)
    	# make sure they are the same!
    	self.assertEqual(badoutput.val,value.val)

if __name__=='__main__':
    unittest.main()