package com.igorbunova

import junit.framework.TestCase
import kotlinx.coroutines.*
import org.apache.commons.codec.digest.DigestUtils
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteIfExists
import kotlin.io.path.toPath


class AsynchronousFileChannelExtensionsTest : TestCase() {

    private lateinit var target: Path
    private lateinit var source: Path

    @OptIn(ExperimentalPathApi::class)
    override fun setUp() {
        val path = javaClass.getResource("test.png")?.toURI()?.toPath()
        assertNotNull(path)
        source = path!!
        target = Files.createTempFile("CoroutinesTest", "write")
    }

    @OptIn(ExperimentalPathApi::class)
    override fun tearDown() {
        target.deleteIfExists()
    }

    @OptIn(ExperimentalPathApi::class)
    fun test() {
        val sourceChannel = AsynchronousFileChannel.open(source, StandardOpenOption.READ)
        val targetChannel = AsynchronousFileChannel.open(target, StandardOpenOption.WRITE)

        val bufferSize = 256
        val content = sourceChannel.read(bufferSize)

        //in real code examples there will be something like list of Job in case of files dispatchers
        //or the calling function will be also suspended
        runBlocking {
            targetChannel.write(content)
        }

        assertFalse(sourceChannel.isOpen)
        assertFalse(targetChannel.isOpen)
    }

    @OptIn(ExperimentalPathApi::class)
    fun testWithDigest() {
        val sourceChannel = AsynchronousFileChannel.open(source, StandardOpenOption.READ)
        val targetChannel = AsynchronousFileChannel.open(target, StandardOpenOption.WRITE)

        val bufferSize = 256
        val content = sourceChannel.read(bufferSize)

        val md5Hash = runBlocking {
            targetChannel.write(content, "MD5")
        }
        val expectedMd5Hash = Files.newInputStream(source).use {
            DigestUtils.md5Hex(it)
        }

        assertEquals(expectedMd5Hash, md5Hash)

        assertFalse(sourceChannel.isOpen)
        assertFalse(targetChannel.isOpen)
    }
}