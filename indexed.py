import sys
import struct
import pathlib
from numbers import Number

_fmt_table = {
# intsize  fmt1   fmt4     MAGIC
    2:    ("!H", "!HHHH", 0xd8fd),
    4:    ("!L", "!LLLL", 0xd8fd2372),
    8:    ("!Q", "!QQQQ", 0xd8fd23720dc4b9f4),
    }

def _all_1s(size):
    "Return size bytes set to all 1"
    v = 0
    for i in range(size):
        v = (v << 8) | 0xff
    return v

INTSIZE = 4
INTBINFORMAT, HEADERFORMAT, MAGIC_NUMBER = _fmt_table[INTSIZE]
NO_MORE_RECORDS = _all_1s(INTSIZE)
DEFAULT_RECORD_SIZE = 512
RECORDS_OFFSET = 4 * INTSIZE    # start of records area

class IndexedFileError(Exception):
    pass

class IndexedFile:
    """
    Simple indexed files.

    Format:
Byte:   0<-- INTSIZE-> <-INTSIZE-> <-- INTSIZE -> <-- INTSIZE ---->
Header  +-------------+-----------+--------------+-----------------+
        | Magic       | Rec. Size | Index offset | 1st free record |
Records +-------------+-----------+--------------+-----------------+
   +--- | next record | Record (recsize - INTSIZE bytes)           |
   |    +-------------+                                            |
   |    |                                                          |
   +--> +-------------+--------------------------------------------+
   +--- | next record | Record                                     |
   |    +-------------+
   +-->    ......                                 INTSIZE   INTSIZE
        +-------------+--------------------------+---------+-------+
Index   | Key Length  | Key (Key Lenght bytes)   | Rec.Idx | Size  |
        +-------------+--------------------------+---------+-------+
        | Key Length  | Key                      | Rec.Idx | Size  |
        +-------------+--------------------------+---------+-------+
         ......
        +-------------+
End     | 0x00000000  |
        +-------------+

    """


    def __init__(self, name, recordsize=0, num_recs_hint=100):
        self.name = name
        self.path = pathlib.Path(name)
        self.index = {}
        if self.path.is_file():
            self.open()
        elif recordsize:
            self.recordsize = max(recordsize, 2 * INTSIZE)
            self.current_size = num_recs_hint*recordsize
            self.index_offset = self.current_size + 4 * INTSIZE
            self.first_free = 0
            self.create()
        else:
            raise IndexedFileError("'{}' Need recordsize for object "
                                "creation.".format(self.name))


    def _read_header(self):
        buf = self.fd.read(4*INTSIZE)
        magic, rs, idxoffs, fstfree = struct.unpack(buf, HEADERFORMAT)
        if magic != MAGIC_NUMBER:
            raise IndexedFileError("'{}' Bad magic number."
                                    .format(self.name))
        self.recordsize = rs
        self.index_offset = idxoffs
        self.first_free = fstfree
        self.current_size = self.index_offset - 8 * INTSIZE


    def _write_header(self):
        buf = struct.pack(HEADERFORMAT,
                    MAGIC_NUMBER,
                    self.recordsize,
                    self.index_offset,
                    self.first_free)
        self.fd.seek(0)
        self.fd.write(buf)


    def open(self):
        self.fd = self.path.open("r+b")
        self.read_header()
        self._read_index()


    def create(self):
        self.fd = self.path.open("w+b")
        self.fd.truncate(self.index_offset)
        self._write_header()
        self.init_free_list()
        self.fd.seek(0, 2) # position to end of file
        self._writeint(0)


    def _readint(self):
        intblock = self.fd.read(INTSIZE)
        return struct.unpack(INTBINFORMAT, intblock)[0]


    def _writeint(self, value):
        self.fd.write(struct.pack(INTBINFORMAT, value))


    def record_number(self, i):
        """Offset of record #i"""
        return i * self.recordsize + RECORDS_OFFSET


    def first_record(self):
        """Offset of first record (record 0)"""
        return self.record_number(0)


    def last_record(self):
        """Offset of last record"""
        num_records = self.current_size // self.recordsize
        return self.record_number(num_records-1)


    def init_free_list(self, start=0):
        num_records = self.current_size // self.recordsize
        for rn in range(start, num_records):
            self.fd.seek(self.record_number(rn))
            self._writeint(rn + 1)

        self.fd.seek(self.last_record())
        self._writeint(NO_MORE_RECORDS)


    def _read_index(self):
        self.fd.seek(self.index_offset)
        keysize = self._readint()
        while keysize:
            key = self.read(keysize)
            idx = self._readint()
            datasize = self._readint()
            self.index[key] = (idx, datasize)
            keysize = self._readint()


    def _write_index(self):
        self.fd.seek(self.index_offset)
        for key in self.index:
            idx, size = self.index[key]
            if isinstance(key, str):
                keybuf = key.encode()
            elif isinstance(key, bytes):
                keybuf = key
            else:
                raise IndexedFileError("'{}' cannot handle keys of type"
                            " '{}' ({}). Must be str or bytes."
                            .format(self.name, type(key), key))
            self._writeint(len(keybuf))
            self.fd.write(keybuf)
            self._writeint(idx)
            self._writeint(size)
        self._writeint(0)


    def close(self):
        self._write_index()
        self.fd.close()


    def allocate(self, numrecords):
        n = 1
        indices = [ self.first_free ]
        last = self.first_free
        while n < numrecords:
            self.fd.seek(self.record_number(last))
            last = self._readint()
            indices.append(last)
            n += 1

        self.fd.seek(self.record_number(last))
        new_first_free = self._readint()
        self.fd.seek(self.record_number(last))
        self._writeint(NO_MORE_RECORDS)
        self.first_free = new_first_free

        self._write_header()
        return indices


    def __setitem__(self, key, bytesval):
        datasize = len(bytesval)
        usable_rec_size = self.recordsize - INTSIZE
        records_needed = datasize // usable_rec_size + 1
        start = 0
        first_record = 0
        for idx in self.allocate(records_needed):
            if start == 0:
                first_record = idx
            self.fd.seek(self.record_number(idx)+INTSIZE)
            self.fd.write(bytesval[start:start+usable_rec_size])
            start += usable_rec_size

        self.index[key] = (first_record, datasize)
        self._write_index()
