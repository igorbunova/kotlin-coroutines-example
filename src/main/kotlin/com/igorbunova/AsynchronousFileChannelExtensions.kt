package com.igorbunova

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.security.MessageDigest
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

//returns number of bytes read
suspend fun AsynchronousFileChannel.asyncRead(buf: ByteBuffer, pos: Long): Int =
    suspendCoroutine { cont ->
        read(buf, pos, Unit, object : CompletionHandler<Int, Unit> {
            override fun completed(bytesRead: Int, attachment: Unit) {
                cont.resume(bytesRead)
            }

            override fun failed(exception: Throwable, attachment: Unit) {
                cont.resumeWithException(exception)
            }
        })
    }

// returns next position
suspend fun AsynchronousFileChannel.asyncWrite(buf: ByteBuffer, pos: Long): Long =
    suspendCoroutine { cont ->
        write(buf, pos, pos, object : CompletionHandler<Int, Long>{
            //bytesRead is number of bytes consumed from buffer
            override fun completed(bytesRead: Int, attachment: Long) {
                cont.resume(attachment + bytesRead)
            }

            override fun failed(exception: Throwable, attachment: Long) {
                cont.resumeWithException(exception)
            }
        })
    }

suspend fun AsynchronousFileChannel.write(content: Flow<ByteArray>) {
    use {
        content.fold(0L) { pos, buf ->
            asyncWrite(ByteBuffer.wrap(buf), pos)
        }
    }
}

suspend fun AsynchronousFileChannel.write(content: Flow<ByteArray>, digestAlg: String): String {
    val md = MessageDigest.getInstance(digestAlg)
    use {
        content.fold(0L) { pos, buf ->
            val next = asyncWrite(ByteBuffer.wrap(buf), pos)
            buf.forEach { md.update(it) }
            return@fold next
        }
    }
    return md.digest().fold(""){ str, b -> str + "%02x".format(b)}
}

fun AsynchronousFileChannel.read(bufferSize: Int): Flow<ByteArray> {
    val buf = ByteBuffer.allocate(bufferSize)
    return flow {
        use {
            var pos = 0L
            while (true) {
                buf.clear()
                val rc = asyncRead(buf, pos)
                if (rc==-1) {
                    break
                }
                pos += rc
                buf.flip()
                if (rc < bufferSize) {
                    val ba = ByteArray(rc)
                    buf.get(ba)
                    emit(ba)
                } else {
                    emit(buf.array())
                }
            }
        }
    }
}