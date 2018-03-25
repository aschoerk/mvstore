# mvstore
PageStore and Implementation of Btree in kotlin for entries of size up to 2048 Bytes.

Currently working on MVCC 

## next TODOs

* simple100BTreeTraTest
* fix page leak because of preImages
* speed up getSortedEntries
    * introduce/maintain sortedflag
    * keep index sorted
    * only unmarshal what is necessary for binary search

## Principles

* Data is stored memory mapped
* Memorymap is organized in pages of constant size (PAGESIZE)
* Memorymap can be recognized as mvstore by MAGIC-Value in page 0
* Beginning at page 1, there is a bit-map describing free/used state of pages.
The number of pages used for this is depending on the length of the 
original region 

# MVCC

Snapshot-Isolation is implemented using PostImages and PreImages.
Each access to a page of a certain number during a transaction is mapped according to some factors:

* The transaction phase: 
   * No Transaction
   * During the transaction
   * During rolling forward
   * During rolling back
   * Pre- And Postimage-Maintenance
* An entry in the page, which signifies which transaction did the last change on that page
* Flags in the page signifiing whether it is a Post-, Preimage or normal/valid page
* A number n or timestamp the current transaction is assigned with. This number
    * allows it to find out which transaction created it t(n)
    * is ordered according to the creation time
    
Some statements:

* Postimages belong to exactly one transaction
* PreImages might be necessary for several transactions. Rules:
   

* Phase: No Transaction
   * Page with certain number gets accessed as such. No mapping takes place
   * Accessed pages must not be pre- or postimages
   * therefore: pages without preimage and postimage flags together must form the consistent file 
   on which transactions are active
   
* Phase: During Transaction m with id m-id
   *  Access of page with number n
      * look for postimage(m, n) if exists, use that
          * then t(m-id) == t(postimage(m, n)) postimage must be true, page can be changed
      * if m-id >= id(n) then: not preimage or postimage: you can use that page
      * search for youngest preimage(n) with id(preimage(n)) < m-id  
      
   * Change of page with number n
      * find out which page must be accessed according to rules above
      * if page is postimage, just change it further
      * create postimage and change that
      * write m-id into postimage-page
      

* Phase: Rolling forward transaction with id m-id
   * Locking??
   * Access of page with number n    
      * if m-id >= n then: not preimage or postimage: you can use that page
      * search for youngest preimage(n) with preimage(n) < m-id  
      
   * Change of page with number n
      * create preimage if it does not exist yet
      * write m-id into postimage-page

   * End of tra
      * free all postimage-pages
      
* Phase: Rolling back transaction with id m-id
   * End of tra
      * free all postimage-pages
   
## Usability of preimage-pages

### example1

Running Tra: t1 
Done Tra: t2

t1 < t2, t2 left preimage p(no_id) p now has id: id(t2)
End of t1 must delete p(no_id) because id(org(p)) <= minimal transaction_id (no transaction running animore)

### example2

Running Tra: t1, t3 
Done Tra: t2

t1,t3 < t2, t2 left preimage p(no_id) p now has id: id(t2)
End of t1 must not delete p(no_id) because id(org(p)) > minimal transaction_id id(t3)
End of t3 must delete p(no_id) because id(org(p)) <= minimal transaction_id (no transaction running animore)

### example3

Running Tra: t1 
Done Tra: t2 > t3 left preimage p(no_id)
Done Tra: t3  uses preimage p(no_id) in case of change: created no additional preimage

End of t1 must delete p(no_id) because id(org(p)) <= minimal transaction_id (no transaction running animore)

### example4

Running Tra: t1 
Done Tra: t2 left preimage p(no_id)
Start/End Tra: t3 reads and changes p and leaves no additional preimage for t1, since id(p) > id (t1)

### example5

Running Tra: t1 
Done Tra: t2 left preimage p(no_id)
Start Tra: t3 
Start/End Tra: t4 reads and changes p and leaves additional preimage for t3, since id(p) <= id (t3)
End Tra: t3 delete preimage p(id(t2)) since for all t: id(t) < id(t2)
 
    

## Building Blocks


### MMapDbFile

Contains one MMapPageFile initialises the freespace upon creation if no MAGIC is 
found at offset 4 in Page 0.
In page 0 at offset 32 the number of the first page of a directory is
stored.
The directory itself is a MMapBTree consisting of 
* the name as key 
* first page as first value
* mvcc as second value


### MMapPageFile

Is a memory mapped file as store. 
Therefore it is able to work as well as in-memory as persistent
it is based on the management of the continuous memory region with 

* page 0
* free-pages map
* directory-btree

The access to this object is normally given by providing an interface 
*IMMapPageFile*

### MMapPageFilePage

Page that are not page 0 or freespace-bitmaps are organized similar 
to postgres file pages. At the beginning there is a fixed size header, 
after that there is a variable sized directory which ends at a specific point.
The other space is occupied by the stored data. 
A page gets filled from top to the end of the directory .
The directory contains information 
* if the entry is deleted, therefore describing freespace in the page
* where an entry starts 
* what the length of the entry is.

### MMapBTree

Is implemented as BTree. It is able to store entries whose added binary length of key 
and value is smaller than pagesize - constant. But storing entries of this size or even 
half of PAGESIZE leads to degenerated trees. So as long as there is no overflow-handling, the combined binary sizes 
should not be more that about 1024 Bytes.


### MVCC/Transactions

At the moment the mvcc-management is realised by encapsulating the 
access of MMapBtree(MVCCBTree) and MMapPageFiles(MVCCFile). This way, without knowledge of 
the clients which use the interface, mvcc can be implemented and triggered by context-specific transaction-handling

#### Snapshotisolation

Isolation on MVCC-Wrapped MMapPageFiles is implemented in the following way:

If a transaction is active, each change of a page leads to copying of this page first, 
if it has not been changed during the transaction yet. From this moment on until the commit of 
the transaction, the new page is used by the changing client.

During commit for each changed page a preimage is created, the original page is overwritten 
and contains the transactionId which did the write. Reading transactions from that moment on can decide,
whether the page is to new and use the preimage if necessary.

#### Commit

The described isolation-mechanism can lead to problems if more than one transaction change the same page.

Therefore the BTree-Encapsulation additionally keeps a log of the operation which have been executed 
on the Tree. 

During commit the originally changed pages get dropped and a state "committing" is started. 
During this state no other client is able to do changes on the BTree. The MMapPageFile will do all changes in the original page 
and create a preimage, which can be used by transactions running in parallel.

#### Rollback

All change pages of the current transactions are freed

#### Two-Phase-Commit

Prepare is not implemented yet. 
