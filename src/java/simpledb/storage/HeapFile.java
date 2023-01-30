package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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

    private final File file;

    private final TupleDesc tupleDesc;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNumber = pid.getPageNumber();
        int tableId = pid.getTableId();
        int pageSize = BufferPool.getPageSize();
        if ((long) (pageNumber + 1) * pageSize > file.length()) {
            throw new IllegalArgumentException();
        }
        try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
            byte[] data = new byte[pageSize];
            int offset = pageNumber * pageSize;
            f.seek(offset);
            int read = f.read(data, 0, pageSize);
            if (read != pageSize) {
                throw new IllegalArgumentException();
            }
            HeapPageId heapPageId = new HeapPageId(tableId, pageNumber);
            return new HeapPage(heapPageId, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int pageNumber = page.getId().getPageNumber();
        int offset = pageSize * pageNumber;
        try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
            f.seek(offset);
            f.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> dirtyPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page != null && page.getNumUnusedSlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                dirtyPages.add(page);
                break;
            }
        }
        // 当前所有页已经满了，需要创建新的页
        if (dirtyPages.isEmpty()) {
            HeapPageId heapPageId = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            writePage(newPage);
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            dirtyPages.add(newPage);
        }
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        List<Page> modifiedPages = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        int tupleNumber = recordId.getTupleNumber();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        if (page != null && page.isSlotUsed(tupleNumber)) {
            page.deleteTuple(t);
            page.markDirty(true, tid);
            modifiedPages.add(page);
        }
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

    private class HeapFileIterator implements DbFileIterator {

        private Iterator<Tuple> iterator;
        private final TransactionId transactionId;
        private final HeapFile heapFile;
        private int pageNumber;

        public HeapFileIterator(TransactionId tid, HeapFile file) {
            this.transactionId = tid;
            this.heapFile = file;
        }

        public Iterator<Tuple> getIterator(int pageNumber) throws DbException, TransactionAbortedException {
            if (pageNumber < 0 || pageNumber >= heapFile.numPages()) {
                throw new DbException("PageNumber " + pageNumber + " is invalid");
            }
            PageId pageId = new HeapPageId(heapFile.getId(), pageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
            if (page == null) {
                throw new DbException("PageNumber " + pageNumber + " is invalid");
            }
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNumber = 0;
            this.iterator = getIterator(this.pageNumber);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                return false;
            }
            while (iterator != null && !iterator.hasNext()) {
                if (pageNumber < heapFile.numPages() - 1) {
                    pageNumber++;
                    iterator = getIterator(pageNumber);
                } else {
                    iterator = null;
                }
            }
            return iterator != null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.iterator = null;
        }
    }

}

