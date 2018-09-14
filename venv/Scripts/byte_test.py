import base64
import struct

"""
Some useful functions for interacting with Java web services from Python.
"""

def make_file_java_byte_array_compatible(file_obj):
    """ 
    Reads in a file and converts it to a format accepted as Java byte array 
    :param file object
    :return string
    """
    encoded_data = base64.b64encode(file_obj.read())
    print encoded_data
    strg = ''
    for i in xrange((len(encoded_data)/40)+1):
        strg += encoded_data[i*40:(i+1)*40]

    return strg


def java_byte_array_to_binary(file_obj):
    """ 
    Converts a java byte array to a binary stream
    :param java byte array as string (pass in as a file like object, can use StringIO)
    :return binary string
    """
    decoded_data = base64.b64decode(file_obj.read())
    strg = ''
    for i in xrange((len(decoded_data)/40)+1):
        strg += decoded_data[i*40:(i+1)*40]

    return strg

# base64
# a = base64.b32encode(1)



# a = (1).to_bytes(2,byteorder = 'big')
#
#
# # a = bin(1)



# num1 = int.from_bytes(b'12', byteorder = 'big')
# num2 = int.from_bytes(b'12', byteorder = 'little')
# print('(%s,'%'num1', num1, '),', '(%s,'%'num2', num2, ')')


# a=six.int2byte(196)

# a = bytes(196)



def allToBytes(ptId,appkey,md,contentId,date):
    b = bytearray([0, 0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,0, 0, 0, 0])  # init
    b[3] = ptId & 0xFF
    ptId >>= 8
    b[2] = ptId & 0xFF
    ptId >>= 8
    b[1] = ptId & 0xFF
    ptId >>= 8
    b[0] = ptId & 0xFF

    b[7] = appkey & 0xFF
    appkey >>= 8
    b[6] = appkey & 0xFF
    appkey >>= 8
    b[5] = appkey & 0xFF
    appkey >>= 8
    b[4] = appkey & 0xFF

    b[11] = md & 0xFF
    md >>= 8
    b[10] = md & 0xFF
    md >>= 8
    b[9] = md & 0xFF
    md >>= 8
    b[8] = md & 0xFF

    b[19] = contentId & 0xFF
    contentId >>= 8
    b[18] = contentId & 0xFF
    contentId >>= 8
    b[17] = contentId & 0xFF
    contentId >>= 8
    b[16] = contentId & 0xFF
    contentId >>= 8
    b[15] = contentId & 0xFF
    contentId >>= 8
    b[14] = contentId & 0xFF
    contentId >>= 8
    b[13] = contentId & 0xFF
    contentId >>= 8
    b[12] = contentId & 0xFF

    b[23] = date & 0xFF
    date >>= 8
    b[22] = date & 0xFF
    date >>= 8
    b[21] = date & 0xFF
    date >>= 8
    b[20] = date & 0xFF

    # Return the result or as bytearray or as bytes (commented out)
    ##return bytes(b)  # uncomment if you need
    return b


# number = 128
# bytestring = number.to_bytes(2, 'little')
# print("integer {int} in bytes is {bytes}".format(int=number, bytes=bytestring))


# a = intToBytes(1,888888,1,123456,20180101)
# print a
# for i in a:
#     print i
#
# a = struct.pack('>i',5)

# a = []
# struct.pack_into('>i',a,0,5)


# print a
# for i in a:
#     print i


def intToBytes(buffer,n):
    tmp = []
    for i in range(4):
        tmp.insert(0,n & 0xFF)
        n >>= 8

    for value in tmp:
        buffer.append(value)

def longToBytes(buffer,n):
    tmp = []
    for i in range(8):
        tmp.insert(0,n & 0xFF)
        n >>= 8

    for value in tmp:
        buffer.append(value)

def allToBytes(ptId,appkey,md,contentId,date):
    b = bytearray([])  # init
    ptId = int(ptId)
    appkey = int(appkey)
    md = int(md)
    contentId = long(contentId)
    intToBytes(b,ptId)
    intToBytes(b,appkey)
    intToBytes(b,md)
    longToBytes(b,contentId)
    date = int(date)
    intToBytes(b,date)

    return b

a = allToBytes('1','888888','1','123456','20180101')


# a = bytearray([])  # init
# # a.append(5)
print len(a)

# intToBytes(a,5)

for i in a:
    print i
