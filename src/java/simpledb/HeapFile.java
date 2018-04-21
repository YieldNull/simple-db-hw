package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc desc;
    private RandomAccessFile raFile;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        desc = td;

        try {
            raFile = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Db file not found");
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return desc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here

        try {
            raFile.seek(BufferPool.getPageSize() * pid.getPageNumber());
            byte[] buffer = new byte[BufferPool.getPageSize()];
            raFile.read(buffer);

            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), buffer);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        raFile.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        raFile.write(page.getPageData());
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i),
                    Permissions.READ_ONLY);

            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return new ArrayList<>(Collections.singletonList(page));
            }
        }

        HeapPage page = new HeapPage(new HeapPageId(getId(), numPages()), new byte[BufferPool.getPageSize()]);
        page.insertTuple(t);

        writePage(page); // new page

        return new ArrayList<>(Collections.singletonList(page));

    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_ONLY);

        page.deleteTuple(t);

        return new ArrayList<>(Collections.singletonList(page));
    }


    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            Iterator<Tuple> iterator;
            int pageNo;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                rewind();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iterator == null) return false;

                while (!iterator.hasNext()) {
                    if ((pageNo + 1) < numPages()) {
                        pageNo++;
                        iterator = ((HeapPage) Database.getBufferPool()
                                .getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY))
                                .iterator();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageNo = 0;
                HeapPage page = (HeapPage) readPage(new HeapPageId(getId(), pageNo));
                iterator = page.iterator();
            }

            @Override
            public void close() {
                iterator = null;
                pageNo = numPages();
            }
        };
    }
}

