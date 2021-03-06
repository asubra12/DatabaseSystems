import struct
import re

# Construct objects w/ fields corresponding to columns.
# Store fields using the appropriate representation:
# TEXT => bytes
# DATE => bytes
# INTEGER => int
# FLOAT => float

class Lineitem(object):
    # The format string, for use with the struct module.
    fmt = "IIIIffffss10s10s10s25s10s44s"

    # Initialize a lineitem object.
    # Arguments are strings that correspond to the columns of the tuple.
    # Feel free to use __new__ instead.
    # (e.g., if you decide to inherit from an immutable class).
    def __init__(self, *args):
        args = list(args)
        self.fmt = "IIIIffffss10s10s10s25s10s44s"
        self.l_orderkey = self.typecast(args[0])
        self.l_partkey = self.typecast(args[1])
        self.l_suppkey = self.typecast(args[2])
        self.l_linenumber = self.typecast(args[3])
        self.l_quantity = self.typecast(args[4])
        self.l_extendedprice = self.typecast(args[5])
        self.l_discount = self.typecast(args[6])
        self.l_tax = self.typecast(args[7])
        self.l_returnflag = self.typecast(args[8])
        self.l_linestatus = self.typecast(args[9])
        self.l_shipdate = self.typecast(args[10])
        self.l_commitdate = self.typecast(args[11])
        self.l_receiptdate = self.typecast(args[12])
        self.l_shipinstruct = self.typecast(args[13])
        self.l_shipmode = self.typecast(args[14])
        self.l_comment = self.typecast(args[15])
        return

    def typecast(self, string):
        datematch = re.compile('^\d+-\d+-\d+$')
        floatmatch = re.compile('^\d+\.\d+$')
        intmatch = re.compile('^\d+$')

        if datematch.match(string) is not None:
            return string.encode('utf-8')
        elif floatmatch.match(string) is not None:
            return float(string)
        elif intmatch.match(string) is not None:
            return int(string)
        else:
            return string.encode('utf-8')


    def pad(self, string, size):
        curr_len = len(string)
        new_str = ((size-curr_len)*' ') + string
        return new_str

    # Pack this lineitem object into a bytes object.
    def pack(self):
        endPack = struct.pack(self.fmt,
                              self.l_orderkey,
                              self.l_partkey,
                              self.l_suppkey,
                              self.l_linenumber,
                              self.l_quantity,
                              self.l_extendedprice,
                              self.l_discount,
                              self.l_tax,
                              self.l_returnflag,
                              self.l_linestatus,
                              self.l_shipdate,
                              self.l_commitdate,
                              self.l_receiptdate,
                              self.pad(self.l_shipinstruct.decode('utf-8'), 25).encode('ascii'),
                              self.pad(self.l_shipmode.decode('utf-8'), 10).encode('ascii'),
                              self.pad(self.l_comment.decode('utf-8'), 44).encode('ascii'))

        return endPack

    # Construct a lineitem object from a bytes object.
    @classmethod
    def unpack(cls, byts):
        fmt = "IIIIffffss10s10s10s25s10s44s"
        lst = []
        unpacked = struct.unpack(fmt, byts)

        for a in unpacked:
            if type(a) == int or type(a) == float:
                lst.append(str(a))
            else:
                lst.append(a.decode('utf-8').lstrip())

        return cls(*lst)

    # Return the size of the packed representation.
    # Do not change.
    @classmethod
    def byteSize(cls):
        return struct.calcsize(cls.fmt)


class Orders(object):
    # The format string, for use with the struct module.
    fmt = "I I s f 10s 15s 15s I 79s"

    # Initialize an orders object.
    # Arguments are strings that correspond to the columns of the tuple.
    # Feel free to use __new__ instead.
    # (e.g., if you decide to inherit from an immutable class).
    def __init__(self, *args):
        args = list(args)
        self.fmt = "I I s f 10s 15s 15s I 79s"
        self.o_orderkey = self.typecast(args[0])
        self.o_custkey = self.typecast(args[1])
        self.o_orderstatus = self.typecast(args[2])
        self.o_totalprice = self.typecast(args[3])
        self.o_orderdate = self.typecast(args[4])
        self.o_orderpriority = self.typecast(args[5])
        self.o_clerk = self.typecast(args[6])
        self.o_shippriority = self.typecast(args[7])
        self.o_comment = self.typecast(args[8])
        return

    def typecast(self, string):
        datematch = re.compile('^\d+-\d+-\d+$')
        floatmatch = re.compile('^\d+\.\d+$')
        intmatch = re.compile('^\d+$')

        if datematch.match(string) is not None:
            return string.encode('utf-8')
        elif floatmatch.match(string) is not None:
            return float(string)
        elif intmatch.match(string) is not None:
            return int(string)
        else:
            return string.encode('utf-8')


    def pad(self, string, size):
        curr_len = len(string)
        new_str = ((size-curr_len)*' ') + string
        return new_str

    # Pack this orders object into a bytes object.
    def pack(self):
        endPack = struct.pack(self.fmt,
                              self.o_orderkey,
                              self.o_custkey,
                              self.o_orderstatus,
                              self.o_totalprice,
                              self.pad(self.o_orderdate.decode('utf-8'), 10).encode('ascii'),
                              self.pad(self.o_orderpriority.decode('utf-8'), 15).encode('ascii'),
                              self.pad(self.o_clerk.decode('utf-8'), 15).encode('ascii'),
                              self.o_shippriority,
                              self.pad(self.o_comment.decode('utf-8'), 79).encode('ascii'))
        return endPack

    # Construct an orders object from a bytes object.
    @classmethod
    def unpack(cls, byts):
        fmt = "I I s f 10s 15s 15s I 79s"
        lst = []
        unpacked = struct.unpack(fmt, byts)

        for a in unpacked:
            if type(a) == int or type(a) == float:
                lst.append(str(a))
            else:
                lst.append(a.decode('utf-8').lstrip())

        return cls(*lst)


    # Return the size of the packed representation.
    # Do not change.
    @classmethod
    def byteSize(cls):
        return struct.calcsize(cls.fmt)

# Return a list of 'cls' objects.
# Assuming 'cls' can be constructed from the raw string fields.
def readCsvFile(inPath, cls, delim='|'):
    lst = []
    with open(inPath, 'r') as f:
        for line in f:
            fields = line.strip().split(delim)
            lst.append(cls(*fields))
    return lst

# Write the list of objects to the file in packed form.
# Each object provides a 'pack' method for conversion to bytes.
def writeBinaryFile(outPath, lst):
    target = open(outPath, 'wb')
    for i in lst:
      target.write(i.pack())
      # target.write(bytes('\n'))
    target.close()
    return


# Read the binary file, and return a list of 'cls' objects.
# 'cls' provides 'byteSize' and 'unpack' methods for reading and conversion.
def readBinaryFile(inPath, cls):
    f = open(inPath, 'rb')
    data = f.read()
    bytesize = cls.byteSize()
    cls_obj = []
    tot = int(len(data) / bytesize)

    for i in range(tot):
        to_unpack = data[(i*bytesize):((i+1)*bytesize)]
        temp = cls.unpack(to_unpack)
        cls_obj.append(temp)

    return cls_obj
