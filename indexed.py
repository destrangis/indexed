import sys
import struct
import pathlib
import pickle
from numbers import Number

INTSIZE = 4
INTBINFORMAT = "!L"
HEADERFORMAT = "!LLLL"
MAGIC_NUMBER = 0xd8fd2372
NO_MORE_RECORDS = 0xffffffff
DEFAULT_RECORD_SIZE = 512
DEFAULT_NUM_RECORDS = 10
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


    def __init__(self, name, mode='r',
                        recordsize=DEFAULT_RECORD_SIZE,
                        num_recs_hint=DEFAULT_NUM_RECORDS):
        if mode not in "rc":
            raise IndexedFileError("'{}' Mode must be 'r' or 'c'")
        self.name = name
        self.path = pathlib.Path(name)
        self.index = {}
        if mode == "r":
            if self.path.is_file():
                self.open()
            else:
                raise IndexedFileError("'{}' Not found.")
        elif mode == "c":
            self.recordsize = max(recordsize, 2 * INTSIZE)
            self.current_size = num_recs_hint*recordsize
            self.index_offset = self.current_size + RECORDS_OFFSET
            self.first_free = 0
            self.create()


    def _read_header(self):
        self.fd.seek(0)
        buf = self.fd.read(4*INTSIZE)
        magic, rs, idxoffs, fstfree = struct.unpack(HEADERFORMAT, buf)
        if magic != MAGIC_NUMBER:
            raise IndexedFileError("'{}' Bad magic number."
                                    .format(self.name))
        self.recordsize = rs
        self.index_offset = idxoffs
        self.first_free = fstfree
        self.current_size = self.index_offset - RECORDS_OFFSET


    def _write_header(self):
        buf = struct.pack(HEADERFORMAT,
                    MAGIC_NUMBER,
                    self.recordsize,
                    self.index_offset,
                    self.first_free)
        self.fd.seek(0)
        self.fd.write(buf)
        self.fd.flush()


    def open(self):
        self.fd = self.path.open("r+b")
        self._read_header()
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
            keybytes = self.fd.read(keysize)
            key = pickle.loads(keybytes)
            idx = self._readint()
            datasize = self._readint()
            self.index[key] = (idx, datasize)
            keysize = self._readint()


    def _write_index(self):
        self.fd.seek(self.index_offset)
        for key in self.index:
            idx, size = self.index[key]
            keybytes = pickle.dumps(key)
            self._writeint(len(keybytes))
            self.fd.write(keybytes)
            self._writeint(idx)
            self._writeint(size)
        self._writeint(0)


    def close(self):
        self._write_index()
        self.fd.close()


    def _allocate_records(self, numrecords):
        n = 0
        indices = []
        recnum = self.first_free
        while n < numrecords and recnum != NO_MORE_RECORDS:
            indices.append(recnum)
            n += 1
            self.fd.seek(self.record_number(recnum))
            recnum = self._readint()

        if n < numrecords:
            raise IndexedFileError("Out of space")

        last = indices[-1]
        self.fd.seek(self.record_number(last))
        new_first_free = self._readint()
        self.fd.seek(self.record_number(last))
        self._writeint(NO_MORE_RECORDS)
        self.first_free = new_first_free

        self._write_header()
        return indices

    def allocate(self, numrecords):
        """
        Return a list of free records, resizing the file if not enough
        free records are available.
        """
        free_list = []
        while not free_list:
            try:
                free_list = self._allocate_records(numrecords)
            except IndexedFileError as err:
                if str(err) == "Out of space":
                    self.resize()
                else:
                    raise

        return free_list

    def __setitem__(self, key, bytesval):
        if key in self:
            del self[key]

        datasize = len(bytesval)
        usable_rec_size = self.recordsize - INTSIZE
        records_needed = datasize // usable_rec_size + 1

        start = 0
        free_list = self.allocate(records_needed)
        for idx in free_list:
            self.fd.seek(self.record_number(idx)+INTSIZE)
            self.fd.write(bytesval[start:start+usable_rec_size])
            start += usable_rec_size

        first_record = free_list[0]
        self.index[key] = (first_record, datasize)
        self._write_index()


    def retrieve(self, start):
        """
        Retrieve the data of the records from start until a record
        marked with NO_MORE_RECORDS
        """
        usable_rec_size = self.recordsize - INTSIZE

        idx = start
        while idx != NO_MORE_RECORDS:
            self.fd.seek(self.record_number(idx))
            idx = self._readint()
            yield self.fd.read(usable_rec_size)


    def record_list(self, start):
        """
        Return the record chain from start until NO_MORE_RECORDS
        """
        idx = start
        while True:
            yield idx
            self.fd.seek(self.record_number(idx))
            idx = self._readint()
            if idx == NO_MORE_RECORDS:
                break

    def last_in_chain(self, start):
        """
        Return the last record in a chain starting by start.
        The next_record field in its index should be NO_MORE_RECORDS
        """
        for idx in self.record_list(start):
            pass
        return idx


    def __getitem__(self, key):
        usable_rec_size = self.recordsize - INTSIZE
        first_record, datasize = self.index[key]
        buf = b""
        for chunk in self.retrieve(first_record):
            buf += chunk

        return buf[:datasize]


    def resize(self):
        num_records = self.current_size // self.recordsize
        new_size = 2 * self.current_size
        new_index_offset = new_size + RECORDS_OFFSET
        self.index_offset = new_index_offset
        self.fd.truncate(self.index_offset)
        self.current_size = new_size
        first_new_record = num_records
        self.init_free_list(first_new_record)

        if self.first_free != NO_MORE_RECORDS:
            # not completely full, add new records to existing free space
            idx = self.last_in_chain(self.first_free)
            self.fd.seek(self.record_number(idx))
            self._writeint(first_new_record)
        else:
            self.first_free = first_new_record
        self._write_index()
        self._write_header()


    def __delitem__(self, key):
        first_record, datasize = self.index[key]
        del self.index[key]
        self._write_index()
        idx = self.last_in_chain(first_record)
        self.fd.seek(self.record_number(idx))
        self._writeint(self.first_free)
        self.first_free = first_record
        self._write_header()


    def __contains__(self, key):
        return key in self.index

    def gen_keys(self):
        for k in self.index.keys():
            yield k

    def __iter__(self):
        class IDXFileIter:
            def __init__(self2):
                self2.gkeys = self.gen_keys()
            def __next__(self2):
                return next(self2.gkeys)
        return IDXFileIter()

    def keys(self):
        return self.index.keys()

    def values(self):
        class IDXFileVals:
            def __iter__(self2):
                self2.gkeys = self.gen_keys()
                return self2
            def __next__(self2):
                k = next(self2.gkeys)
                return self[k]
        return IDXFileVals()

    def items(self):
        class IDXFileItems:
            def __iter__(self2):
                self2.gkeys = self.gen_keys()
                return self2
            def __next__(self2):
                k = next(self2.gkeys)
                return k, self[k]
        return IDXFileItems()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
