package com.connection.socket.server

import com.connection.socket.FrameSocket
import com.connection.socket.SocketListener

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.ServerSocket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.readUTF8Line
import io.ktor.utils.io.writeByteArray
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.io.InternalIoApi

class ServerSocketTCP(private val port: Int) {
    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    var serverSocket: ServerSocket? = null


    private val lastDatagramData = MutableStateFlow<ByteArray>(byteArrayOf())
    val datagramFlow: SharedFlow<ByteArray> = lastDatagramData
    private var listeners = mutableListOf<SocketListener>()
    fun addListener(listener: SocketListener) {
        listeners.add(listener)
    }

    fun removeListener(listener: SocketListener) {
        listeners.remove(listener)
    }

    private fun onDatagramReceived(datagram: ByteArray) {
        listeners.forEach { listener ->
            listener.onDatagramReceived(datagram)
        }
    }


    var datagramSocketFlow = MutableSharedFlow<FrameSocket>(
        extraBufferCapacity = 1
    )

    @OptIn(InternalIoApi::class)
    fun start() {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                serverSocket = aSocket(selectorManager).tcp().bind("0.0.0.0", port)

                println("Server is listening at ${serverSocket!!.localAddress}")
                try {
                    while (true) {
                        val socket = serverSocket!!.accept()
                        launch {
                            val readChannel = socket.openReadChannel()
                            try {
                                while (true) {
//                                    val line = readChannel.readUTF8Line()
//                                    if (line == null) break
//                                    println("Received: $line")

                                    val datagramValue = readChannel.readByteArray(4096)
                                    onDatagramReceived(datagramValue)
                                    lastDatagramData.value = datagramValue
                                }
                            } finally {
                                socket.close()
                            }
                        }
                    }
                } catch (ex: Exception) {
                    println(ex.message)
                }
            } catch (ex: Exception) {
                println(ex.message)
            }
        }
        // myJob?.start()
    }

    val mutex = Mutex()
    var processing = false


    fun startForSend() {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                serverSocket = aSocket(selectorManager).tcp().bind("0.0.0.0", port)

                println("Server is listening at ${serverSocket!!.localAddress}")

                while (true) {
                    val socket = serverSocket!!.accept()
                    launch {
                        val writeChannel = socket.openWriteChannel(autoFlush = true)
                        println("socketttttttttttttttttttttttttttttttttt")
                        datagramSocketFlow.collect { datagram ->
                            if (processing) return@collect
                            processing = true
                            try {
                                println("${datagram.valor} socketttttttttttttttttttttttttttttttttt")
                                writeChannel.writeByteArray(datagram.tamanho)
                                writeChannel.writeByteArray(datagram.valor)
                            } catch (ex: Exception) {
                                println(ex.message)
                            }
                            processing = false
                        }
                    }
//                    launch {
//                        //val readChannel = socket.openReadChannel()
//                        val writeChannel = socket.openWriteChannel(autoFlush = true)
//                        try {
//
//                            datagramSocketFlow.collect { datagram ->
//                                println(datagram.tamanho)
//                                writeChannel.writeByteArray(datagram.valor)
//                            }
//
////                            while (true) {
////                               // println(readChannel.readUTF8Line())
////                                writeChannel.writeStringUtf8("Hello from Server!\n")
////
//////                                val datagramValue = readChannel.readByteArray(4096)
//////                                onDatagramReceived(datagramValue)
//////                                lastDatagramData.value = datagramValue
////                                // writeChannel.writeStringUtf8("Hello from Server!")
////                                delay(200)
////                            }
//                        } finally {
//                            socket.close()
//                        }
//                    }
                }
            } catch (ex: Exception) {
                println(ex.message)
            }
        }
        // myJob?.start()
    }


    fun stop() {
        myJob?.cancel()
        serverSocket?.close()

    }


}