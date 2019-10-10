###IndexedFile

This is a simple implementation of Indexed files similar, but not identical, to dbm/gdbm/ndbm.

####Usage
Instantiation:

~~~python
from indexed import IndexedFile

db = IndexedFile("database.db", "c")
~~~

Store some data. Keys must be hashable objects, and the content can only be bytes:

~~~python
db[("IDENT", 405)] = b"this is info to be stored"
db[35] = json.dumps({ "name": "Phil", 
                      "address": "35, Barnacle st.", 
                      "date_of_birth": "1982/11/24"}).encode()
~~~

Retrieve data:

~~~python
print(db[("IDENT", 405)])  # prints b'this is info to be stored'
~~~

Bad keys throw `KeyError` exception:

~~~python
db["badkey"]   # KeyError: 'badkey'
~~~

the `.keys()`, `.values()` and `.items()` methods behave like in dictionaries, and the objects are iterable. Iterating over an `IndexedFile` gives the keys in any order:

~~~python
for key in db:
   print(key, end=" ")  # prints 35 ("IDENT", 405)
~~~

Using `.close()` method closes the object and it cannot be used further. To use after `.close()`, a new instance should be created.

~~~python
db.close()
~~~

The objects are context managers:

~~~python
with IndexedFile("database.db", "r") as db:
	print(db[35])
~~~

####Reference
**IndexedFile(name, mode='r', recordsize=DEFAULT_RECORD_SIZE, num_recs_hint=DEFAULT_NUM_RECORDS)**
Create a new IndexedFile object, where `name` is the filename to store it to, `mode` is either 'c' which creates or resets a new file, or 'r' which opens an existing file.

`recordsize` is the size of each record. Data items can be bigger than the recordsize, in which case a suitable number of records will be allocated to contain the object.

`num_recs_hint` is the number of free records initially allocated. When more records are needed than are available the file will be resized to double its size.

recordsize and num_recs_hint, if specified, are ignored on `mode` 'r'.

**IndexedFile.close()**
Closes access to the IndexedFile. The file cannot be re-opened. In order to access the file after `.close()` a new instance should be created with `mode` 'r'

**IndexedFile.keys()**
Returns an iterable that gives the keys in sequence

**IndexedFile.values()**
Returns an iterable that gives the values in sequence

**IndexedFile.items()**
Returns and iterable that gives tuples of the form _(key, value)_

####License
This software is released under the **MIT License**