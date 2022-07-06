package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {
    boolean[] slotFlag;

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;
    TransactionId dirtyTid;
    boolean isDirty;
    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *      设 tupleNum = x
     *      x*tupleSize*8 + x = BufferPool.getPageSize()*8
     *      x个比特作为slot
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {

        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        translationHeader();
        tuples = new Tuple[numSlots];

        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++) {
                tuples[i] = readNextTuple(dis, i);

//                while (tuples[i].fields().hasNext()){
//                    Field next = tuples[i].fields().next();
//                    System.out.print(next.toString()+" ");
//                }
//                System.out.println();
            }

        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();
        setBeforeImage();
        //创建完页之后就将其写入磁盘

    }
    public HeapPage(HeapPageId id) {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.slotFlag = new boolean[numSlots];
        tuples = new Tuple[numSlots];
        header = new byte[getHeaderSize()];
        setBeforeImage();
    }
    private void translationHeader() {
        StringBuilder sb = new StringBuilder(header.length * Byte.SIZE);
        for( int i = 0; i < Byte.SIZE * header.length; i++ )
            sb.append((header[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
        slotFlag = new boolean[numSlots];
        int index = 0;
        for(int i = 8; i<=sb.length();i+=8){
            for (int j = 0; j < 8; j++) {
                if(sb.charAt(i-j-1)=='1') {
                    slotFlag[index] = true;
                }
                index++;
            }
        }
    }

    private void translationSlotFlag(){
        StringBuilder sb = new StringBuilder(header.length * Byte.SIZE);
        for( int i = 0; i < Byte.SIZE * header.length; i++ )
            sb.append((header[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
        int index = 0;
        for(int i = 8; i<=sb.length();i+=8){
            for (int j = 0; j < 8; j++) {
                if(index<slotFlag.length&&slotFlag[index]) {
                    sb.setCharAt(i-j-1,'1');
                }else{
                    sb.setCharAt(i-j-1,'0');
                }
                index++;
            }
        }
    }

    public int getTupleSlotNum(RecordId recordId){
        for (int i = 0; i < numSlots; i++) {
            if(tuples[i].getRecordId().equals(recordId)){
                return i;
            }
        }
        return -1;
    }


    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        // some code goes here

        int tupleSize = td.getSize();
        return (int)Math.floor((double) (BufferPool.getPageSize()*8) / (tupleSize * 8 + 1));

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        

        // some code goes here

        return (int)Math.ceil((double) numSlots/8);

                 
    }


    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        translationSlotFlag();
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }



        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId recordId = t.getRecordId();
        if(recordId.pid != this.pid){
            throw new DbException("the tuple is not on this page");
        }
        int tupleNum = recordId.tupleNum;
        if(tupleNum<0||tupleNum>=slotFlag.length){
            throw new DbException("IllegalTupleNum : "+tupleNum);
        }
        if(!slotFlag[tupleNum]){
            throw new DbException("tuple slot is already empty");
        }
        slotFlag[tupleNum] = false;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {

        if(!this.td.equals(t.getTupleDesc())){
            throw new DbException("tupledesc is mismatch");
        }
        for (int i = 0; i < numSlots; i++) {
            if(!slotFlag[i]){
                tuples[i] = t;
                tuples[i].setRecordId(new RecordId(pid,i));
                slotFlag[i] = true;
                return;
            }
        }
        throw new DbException("the page is full");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.isDirty = dirty;
        if(dirty){
            this.dirtyTid = tid;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        if(isDirty){
            return dirtyTid;
        }else{
            return null;
        }
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int emptyNum = 0;
        for (int i = 0; i < numSlots; i++) {
            if(!slotFlag[i]){
                emptyNum++;
            }
        }
        return emptyNum;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here

        return slotFlag[i];
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {

        // some code goes here
        ArrayList<Tuple> t = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if(slotFlag[i]){
                t.add(tuples[i]);
            }
        }
        return t.iterator();
    }

}

