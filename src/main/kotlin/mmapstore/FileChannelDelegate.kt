package mmapstore

import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

class FileChannelDelegate(val fch: FileChannel): FileChannel() {
    /**
     * Closes this channel.
     *
     *
     *  This method is invoked by the [close][.close] method in order
     * to perform the actual work of closing the channel.  This method is only
     * invoked if the channel has not yet been closed, and it is never invoked
     * more than once.
     *
     *
     *  An implementation of this method must arrange for any other thread
     * that is blocked in an I/O operation upon this channel to return
     * immediately, either by throwing an exception or by returning normally.
     *
     *
     * @throws  IOException
     * If an I/O error occurs while closing the channel
     */
    override fun implCloseChannel() {

    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     *
     *  Bytes are written starting at this channel's current file position
     * unless the channel is in append mode, in which case the position is
     * first advanced to the end of the file.  The file is grown, if necessary,
     * to accommodate the written bytes, and then the file position is updated
     * with the number of bytes actually written.  Otherwise this method
     * behaves exactly as specified by the [WritableByteChannel]
     * interface.
     */
    override fun write(src: ByteBuffer?): Int {
        return fch.write(src)
    }

    /**
     * Writes a sequence of bytes to this channel from a subsequence of the
     * given buffers.
     *
     *
     *  Bytes are written starting at this channel's current file position
     * unless the channel is in append mode, in which case the position is
     * first advanced to the end of the file.  The file is grown, if necessary,
     * to accommodate the written bytes, and then the file position is updated
     * with the number of bytes actually written.  Otherwise this method
     * behaves exactly as specified in the [GatheringByteChannel]
     * interface.
     */
    override fun write(srcs: Array<out ByteBuffer>?, offset: Int, length: Int): Long {
        return fch.write(srcs, offset, length)
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer,
     * starting at the given file position.
     *
     *
     *  This method works in the same manner as the [ ][.write] method, except that bytes are written starting at
     * the given file position rather than at the channel's current position.
     * This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then the file will be
     * grown to accommodate the new bytes; the values of any bytes between the
     * previous end-of-file and the newly-written bytes are unspecified.
     *
     * @param  src
     * The buffer from which bytes are to be transferred
     *
     * @param  position
     * The file position at which the transfer is to begin;
     * must be non-negative
     *
     * @return  The number of bytes written, possibly zero
     *
     * @throws  IllegalArgumentException
     * If the position is negative
     *
     * @throws  NonWritableChannelException
     * If this channel was not opened for writing
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  AsynchronousCloseException
     * If another thread closes this channel
     * while the write operation is in progress
     *
     * @throws  ClosedByInterruptException
     * If another thread interrupts the current thread
     * while the write operation is in progress, thereby
     * closing the channel and setting the current thread's
     * interrupt status
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun write(src: ByteBuffer?, position: Long): Int {
        return fch.write(src, position)
    }

    /**
     * Forces any updates to this channel's file to be written to the storage
     * device that contains it.
     *
     *
     *  If this channel's file resides on a local storage device then when
     * this method returns it is guaranteed that all changes made to the file
     * since this channel was created, or since this method was last invoked,
     * will have been written to that device.  This is useful for ensuring that
     * critical information is not lost in the event of a system crash.
     *
     *
     *  If the file does not reside on a local device then no such guarantee
     * is made.
     *
     *
     *  The <tt>metaData</tt> parameter can be used to limit the number of
     * I/O operations that this method is required to perform.  Passing
     * <tt>false</tt> for this parameter indicates that only updates to the
     * file's content need be written to storage; passing <tt>true</tt>
     * indicates that updates to both the file's content and metadata must be
     * written, which generally requires at least one more I/O operation.
     * Whether this parameter actually has any effect is dependent upon the
     * underlying operating system and is therefore unspecified.
     *
     *
     *  Invoking this method may cause an I/O operation to occur even if the
     * channel was only opened for reading.  Some operating systems, for
     * example, maintain a last-access time as part of a file's metadata, and
     * this time is updated whenever the file is read.  Whether or not this is
     * actually done is system-dependent and is therefore unspecified.
     *
     *
     *  This method is only guaranteed to force changes that were made to
     * this channel's file via the methods defined in this class.  It may or
     * may not force changes that were made by modifying the content of a
     * [&lt;i&gt;mapped byte buffer&lt;/i&gt;][MappedByteBuffer] obtained by
     * invoking the [map][.map] method.  Invoking the [ ][MappedByteBuffer.force] method of the mapped byte buffer will
     * force changes made to the buffer's content to be written.
     *
     * @param   metaData
     * If <tt>true</tt> then this method is required to force changes
     * to both the file's content and metadata to be written to
     * storage; otherwise, it need only force content changes to be
     * written
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun force(metaData: Boolean) {
        fch.force(metaData)
    }


    /**
     * Truncates this channel's file to the given size.
     *
     *
     *  If the given size is less than the file's current size then the file
     * is truncated, discarding any bytes beyond the new end of the file.  If
     * the given size is greater than or equal to the file's current size then
     * the file is not modified.  In either case, if this channel's file
     * position is greater than the given size then it is set to that size.
     *
     *
     * @param  size
     * The new size, a non-negative byte count
     *
     * @return  This file channel
     *
     * @throws  NonWritableChannelException
     * If this channel was not opened for writing
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  IllegalArgumentException
     * If the new size is negative
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun truncate(size: Long): FileChannel {
        return fch.truncate(size)
    }

    /**
     * Acquires a lock on the given region of this channel's file.
     *
     *
     *  An invocation of this method will block until the region can be
     * locked, this channel is closed, or the invoking thread is interrupted,
     * whichever comes first.
     *
     *
     *  If this channel is closed by another thread during an invocation of
     * this method then an [AsynchronousCloseException] will be thrown.
     *
     *
     *  If the invoking thread is interrupted while waiting to acquire the
     * lock then its interrupt status will be set and a [ ] will be thrown.  If the invoker's
     * interrupt status is set when this method is invoked then that exception
     * will be thrown immediately; the thread's interrupt status will not be
     * changed.
     *
     *
     *  The region specified by the <tt>position</tt> and <tt>size</tt>
     * parameters need not be contained within, or even overlap, the actual
     * underlying file.  Lock regions are fixed in size; if a locked region
     * initially contains the end of the file and the file grows beyond the
     * region then the new portion of the file will not be covered by the lock.
     * If a file is expected to grow in size and a lock on the entire file is
     * required then a region starting at zero, and no smaller than the
     * expected maximum size of the file, should be locked.  The zero-argument
     * [.lock] method simply locks a region of size [ ][Long.MAX_VALUE].
     *
     *
     *  Some operating systems do not support shared locks, in which case a
     * request for a shared lock is automatically converted into a request for
     * an exclusive lock.  Whether the newly-acquired lock is shared or
     * exclusive may be tested by invoking the resulting lock object's [ ][FileLock.isShared] method.
     *
     *
     *  File locks are held on behalf of the entire Java virtual machine.
     * They are not suitable for controlling access to a file by multiple
     * threads within the same virtual machine.
     *
     * @param  position
     * The position at which the locked region is to start; must be
     * non-negative
     *
     * @param  size
     * The size of the locked region; must be non-negative, and the sum
     * <tt>position</tt>&nbsp;+&nbsp;<tt>size</tt> must be non-negative
     *
     * @param  shared
     * <tt>true</tt> to request a shared lock, in which case this
     * channel must be open for reading (and possibly writing);
     * <tt>false</tt> to request an exclusive lock, in which case this
     * channel must be open for writing (and possibly reading)
     *
     * @return  A lock object representing the newly-acquired lock
     *
     * @throws  IllegalArgumentException
     * If the preconditions on the parameters do not hold
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  AsynchronousCloseException
     * If another thread closes this channel while the invoking
     * thread is blocked in this method
     *
     * @throws  FileLockInterruptionException
     * If the invoking thread is interrupted while blocked in this
     * method
     *
     * @throws  OverlappingFileLockException
     * If a lock that overlaps the requested region is already held by
     * this Java virtual machine, or if another thread is already
     * blocked in this method and is attempting to lock an overlapping
     * region
     *
     * @throws  NonReadableChannelException
     * If <tt>shared</tt> is <tt>true</tt> this channel was not
     * opened for reading
     *
     * @throws  NonWritableChannelException
     * If <tt>shared</tt> is <tt>false</tt> but this channel was not
     * opened for writing
     *
     * @throws  IOException
     * If some other I/O error occurs
     *
     * @see .lock
     * @see .tryLock
     * @see .tryLock
     */
    override fun lock(position: Long, size: Long, shared: Boolean): FileLock {
        return fch.lock(position, size, shared)
    }

    /**
     * Attempts to acquire a lock on the given region of this channel's file.
     *
     *
     *  This method does not block.  An invocation always returns
     * immediately, either having acquired a lock on the requested region or
     * having failed to do so.  If it fails to acquire a lock because an
     * overlapping lock is held by another program then it returns
     * <tt>null</tt>.  If it fails to acquire a lock for any other reason then
     * an appropriate exception is thrown.
     *
     *
     *  The region specified by the <tt>position</tt> and <tt>size</tt>
     * parameters need not be contained within, or even overlap, the actual
     * underlying file.  Lock regions are fixed in size; if a locked region
     * initially contains the end of the file and the file grows beyond the
     * region then the new portion of the file will not be covered by the lock.
     * If a file is expected to grow in size and a lock on the entire file is
     * required then a region starting at zero, and no smaller than the
     * expected maximum size of the file, should be locked.  The zero-argument
     * [.tryLock] method simply locks a region of size [ ][Long.MAX_VALUE].
     *
     *
     *  Some operating systems do not support shared locks, in which case a
     * request for a shared lock is automatically converted into a request for
     * an exclusive lock.  Whether the newly-acquired lock is shared or
     * exclusive may be tested by invoking the resulting lock object's [ ][FileLock.isShared] method.
     *
     *
     *  File locks are held on behalf of the entire Java virtual machine.
     * They are not suitable for controlling access to a file by multiple
     * threads within the same virtual machine.
     *
     * @param  position
     * The position at which the locked region is to start; must be
     * non-negative
     *
     * @param  size
     * The size of the locked region; must be non-negative, and the sum
     * <tt>position</tt>&nbsp;+&nbsp;<tt>size</tt> must be non-negative
     *
     * @param  shared
     * <tt>true</tt> to request a shared lock,
     * <tt>false</tt> to request an exclusive lock
     *
     * @return  A lock object representing the newly-acquired lock,
     * or <tt>null</tt> if the lock could not be acquired
     * because another program holds an overlapping lock
     *
     * @throws  IllegalArgumentException
     * If the preconditions on the parameters do not hold
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  OverlappingFileLockException
     * If a lock that overlaps the requested region is already held by
     * this Java virtual machine, or if another thread is already
     * blocked in this method and is attempting to lock an overlapping
     * region of the same file
     *
     * @throws  IOException
     * If some other I/O error occurs
     *
     * @see .lock
     * @see .lock
     * @see .tryLock
     */
    override fun tryLock(position: Long, size: Long, shared: Boolean): FileLock {
        return fch.tryLock(position, size, shared)
    }

    /**
     * Returns this channel's file position.
     *
     * @return  This channel's file position,
     * a non-negative integer counting the number of bytes
     * from the beginning of the file to the current position
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun position(): Long {
        return fch.position()
    }

    /**
     * Sets this channel's file position.
     *
     *
     *  Setting the position to a value that is greater than the file's
     * current size is legal but does not change the size of the file.  A later
     * attempt to read bytes at such a position will immediately return an
     * end-of-file indication.  A later attempt to write bytes at such a
     * position will cause the file to be grown to accommodate the new bytes;
     * the values of any bytes between the previous end-of-file and the
     * newly-written bytes are unspecified.
     *
     * @param  newPosition
     * The new position, a non-negative integer counting
     * the number of bytes from the beginning of the file
     *
     * @return  This file channel
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  IllegalArgumentException
     * If the new position is negative
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun position(newPosition: Long): FileChannel {
        return fch.position(newPosition)
    }

    /**
     * Maps a region of this channel's file directly into memory.
     *
     *
     *  A region of a file may be mapped into memory in one of three modes:
     *
     *
     *
     *
     *  *
     *
     * *Read-only:* Any attempt to modify the resulting buffer
     * will cause a [java.nio.ReadOnlyBufferException] to be thrown.
     * ([MapMode.READ_ONLY])
     *
     *  *
     *
     * *Read/write:* Changes made to the resulting buffer will
     * eventually be propagated to the file; they may or may not be made
     * visible to other programs that have mapped the same file.  ([   ][MapMode.READ_WRITE])
     *
     *  *
     *
     * *Private:* Changes made to the resulting buffer will not
     * be propagated to the file and will not be visible to other programs
     * that have mapped the same file; instead, they will cause private
     * copies of the modified portions of the buffer to be created.  ([   ][MapMode.PRIVATE])
     *
     *
     *
     *
     *  For a read-only mapping, this channel must have been opened for
     * reading; for a read/write or private mapping, this channel must have
     * been opened for both reading and writing.
     *
     *
     *  The [&lt;i&gt;mapped byte buffer&lt;/i&gt;][MappedByteBuffer]
     * returned by this method will have a position of zero and a limit and
     * capacity of <tt>size</tt>; its mark will be undefined.  The buffer and
     * the mapping that it represents will remain valid until the buffer itself
     * is garbage-collected.
     *
     *
     *  A mapping, once established, is not dependent upon the file channel
     * that was used to create it.  Closing the channel, in particular, has no
     * effect upon the validity of the mapping.
     *
     *
     *  Many of the details of memory-mapped files are inherently dependent
     * upon the underlying operating system and are therefore unspecified.  The
     * behavior of this method when the requested region is not completely
     * contained within this channel's file is unspecified.  Whether changes
     * made to the content or size of the underlying file, by this program or
     * another, are propagated to the buffer is unspecified.  The rate at which
     * changes to the buffer are propagated to the file is unspecified.
     *
     *
     *  For most operating systems, mapping a file into memory is more
     * expensive than reading or writing a few tens of kilobytes of data via
     * the usual [read][.read] and [write][.write] methods.  From the
     * standpoint of performance it is generally only worth mapping relatively
     * large files into memory.
     *
     * @param  mode
     * One of the constants [READ_ONLY][MapMode.READ_ONLY], [         ][MapMode.READ_WRITE], or [         PRIVATE][MapMode.PRIVATE] defined in the [MapMode] class, according to
     * whether the file is to be mapped read-only, read/write, or
     * privately (copy-on-write), respectively
     *
     * @param  position
     * The position within the file at which the mapped region
     * is to start; must be non-negative
     *
     * @param  size
     * The size of the region to be mapped; must be non-negative and
     * no greater than [java.lang.Integer.MAX_VALUE]
     *
     * @return  The mapped byte buffer
     *
     * @throws NonReadableChannelException
     * If the <tt>mode</tt> is [READ_ONLY][MapMode.READ_ONLY] but
     * this channel was not opened for reading
     *
     * @throws NonWritableChannelException
     * If the <tt>mode</tt> is [READ_WRITE][MapMode.READ_WRITE] or
     * [PRIVATE][MapMode.PRIVATE] but this channel was not opened
     * for both reading and writing
     *
     * @throws IllegalArgumentException
     * If the preconditions on the parameters do not hold
     *
     * @throws IOException
     * If some other I/O error occurs
     *
     * @see java.nio.channels.FileChannel.MapMode
     *
     * @see java.nio.MappedByteBuffer
     */
    override fun map(mode: MapMode?, position: Long, size: Long): MappedByteBuffer {
        return fch.map(mode, position, size)
    }

    /**
     * Returns the current size of this channel's file.
     *
     * @return  The current size of this channel's file,
     * measured in bytes
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun size(): Long {
        return fch.size()
    }

    /**
     * Transfers bytes from this channel's file to the given writable byte
     * channel.
     *
     *
     *  An attempt is made to read up to <tt>count</tt> bytes starting at
     * the given <tt>position</tt> in this channel's file and write them to the
     * target channel.  An invocation of this method may or may not transfer
     * all of the requested bytes; whether or not it does so depends upon the
     * natures and states of the channels.  Fewer than the requested number of
     * bytes are transferred if this channel's file contains fewer than
     * <tt>count</tt> bytes starting at the given <tt>position</tt>, or if the
     * target channel is non-blocking and it has fewer than <tt>count</tt>
     * bytes free in its output buffer.
     *
     *
     *  This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then no bytes are
     * transferred.  If the target channel has a position then bytes are
     * written starting at that position and then the position is incremented
     * by the number of bytes written.
     *
     *
     *  This method is potentially much more efficient than a simple loop
     * that reads from this channel and writes to the target channel.  Many
     * operating systems can transfer bytes directly from the filesystem cache
     * to the target channel without actually copying them.
     *
     * @param  position
     * The position within the file at which the transfer is to begin;
     * must be non-negative
     *
     * @param  count
     * The maximum number of bytes to be transferred; must be
     * non-negative
     *
     * @param  target
     * The target channel
     *
     * @return  The number of bytes, possibly zero,
     * that were actually transferred
     *
     * @throws IllegalArgumentException
     * If the preconditions on the parameters do not hold
     *
     * @throws  NonReadableChannelException
     * If this channel was not opened for reading
     *
     * @throws  NonWritableChannelException
     * If the target channel was not opened for writing
     *
     * @throws  ClosedChannelException
     * If either this channel or the target channel is closed
     *
     * @throws  AsynchronousCloseException
     * If another thread closes either channel
     * while the transfer is in progress
     *
     * @throws  ClosedByInterruptException
     * If another thread interrupts the current thread while the
     * transfer is in progress, thereby closing both channels and
     * setting the current thread's interrupt status
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun transferTo(position: Long, count: Long, target: WritableByteChannel?): Long {
        return fch.transferTo(position, count, target)
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     *
     *  Bytes are read starting at this channel's current file position, and
     * then the file position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the [ ] interface.
     */
    override fun read(dst: ByteBuffer?): Int {
        return fch.read(dst)
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the
     * given buffers.
     *
     *
     *  Bytes are read starting at this channel's current file position, and
     * then the file position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the [ ] interface.
     */
    override fun read(dsts: Array<out ByteBuffer>?, offset: Int, length: Int): Long {
        return fch.read(dsts, offset, length)
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer,
     * starting at the given file position.
     *
     *
     *  This method works in the same manner as the [ ][.read] method, except that bytes are read starting at the
     * given file position rather than at the channel's current position.  This
     * method does not modify this channel's position.  If the given position
     * is greater than the file's current size then no bytes are read.
     *
     * @param  dst
     * The buffer into which bytes are to be transferred
     *
     * @param  position
     * The file position at which the transfer is to begin;
     * must be non-negative
     *
     * @return  The number of bytes read, possibly zero, or <tt>-1</tt> if the
     * given position is greater than or equal to the file's current
     * size
     *
     * @throws  IllegalArgumentException
     * If the position is negative
     *
     * @throws  NonReadableChannelException
     * If this channel was not opened for reading
     *
     * @throws  ClosedChannelException
     * If this channel is closed
     *
     * @throws  AsynchronousCloseException
     * If another thread closes this channel
     * while the read operation is in progress
     *
     * @throws  ClosedByInterruptException
     * If another thread interrupts the current thread
     * while the read operation is in progress, thereby
     * closing the channel and setting the current thread's
     * interrupt status
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun read(dst: ByteBuffer?, position: Long): Int {
        return fch.read(dst, position)
    }

    /**
     * Transfers bytes into this channel's file from the given readable byte
     * channel.
     *
     *
     *  An attempt is made to read up to <tt>count</tt> bytes from the
     * source channel and write them to this channel's file starting at the
     * given <tt>position</tt>.  An invocation of this method may or may not
     * transfer all of the requested bytes; whether or not it does so depends
     * upon the natures and states of the channels.  Fewer than the requested
     * number of bytes will be transferred if the source channel has fewer than
     * <tt>count</tt> bytes remaining, or if the source channel is non-blocking
     * and has fewer than <tt>count</tt> bytes immediately available in its
     * input buffer.
     *
     *
     *  This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then no bytes are
     * transferred.  If the source channel has a position then bytes are read
     * starting at that position and then the position is incremented by the
     * number of bytes read.
     *
     *
     *  This method is potentially much more efficient than a simple loop
     * that reads from the source channel and writes to this channel.  Many
     * operating systems can transfer bytes directly from the source channel
     * into the filesystem cache without actually copying them.
     *
     * @param  src
     * The source channel
     *
     * @param  position
     * The position within the file at which the transfer is to begin;
     * must be non-negative
     *
     * @param  count
     * The maximum number of bytes to be transferred; must be
     * non-negative
     *
     * @return  The number of bytes, possibly zero,
     * that were actually transferred
     *
     * @throws IllegalArgumentException
     * If the preconditions on the parameters do not hold
     *
     * @throws  NonReadableChannelException
     * If the source channel was not opened for reading
     *
     * @throws  NonWritableChannelException
     * If this channel was not opened for writing
     *
     * @throws  ClosedChannelException
     * If either this channel or the source channel is closed
     *
     * @throws  AsynchronousCloseException
     * If another thread closes either channel
     * while the transfer is in progress
     *
     * @throws  ClosedByInterruptException
     * If another thread interrupts the current thread while the
     * transfer is in progress, thereby closing both channels and
     * setting the current thread's interrupt status
     *
     * @throws  IOException
     * If some other I/O error occurs
     */
    override fun transferFrom(src: ReadableByteChannel?, position: Long, count: Long): Long {
        return fch.transferFrom(src, position, count)
    }


}