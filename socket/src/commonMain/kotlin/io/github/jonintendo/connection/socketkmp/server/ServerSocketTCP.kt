package io.github.jonintendo.connection.socketkmp.server

import io.github.jonintendo.connection.socketkmp.FrameSocket
import io.github.jonintendo.connection.socketkmp.SocketListener
import io.github.jonintendo.connection.socketkmp.SocketProperties
import io.github.jonintendo.connection.socketkmp.TipoPacote

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.ServerSocket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.IO
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.io.InternalIoApi

class ServerSocketTCP(private val port: Int) {
    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    var serverSocket: ServerSocket? = null


    private val lastState = MutableStateFlow<SocketProperties>(SocketProperties())
    val lastStateFlow: SharedFlow<SocketProperties> = lastState


    private var listeners = mutableListOf<SocketListener>()
    fun addListener(listener: SocketListener) {
        listeners.add(listener)
    }

    fun removeListener(listener: SocketListener) {
        listeners.remove(listener)
    }

    private fun onDatagramReceived(datagram: ByteArray, tipoPacote: TipoPacote) {
        lastState.update { it.copy(lastDatagramData = datagram, lastTipoPacote = tipoPacote) }
        listeners.forEach { listener ->
            listener.onDatagramReceived(datagram, tipoPacote)
        }
    }

    private fun onSocketConnected(connected: Boolean) {
        lastState.update { it.copy(lastConnectionState = connected) }
        listeners.forEach { listener ->
            listener.onSocketConnected(connected)
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
                onSocketConnected(true)
                println("Server is listening at ${serverSocket!!.localAddress}")

                while (true) {
                    val socket = serverSocket!!.accept()
                    launch {
                        val readChannel = socket.openReadChannel()
                        try {
                            while (true) {
                                try {
//                                    val line = readChannel.readUTF8Line()
//                                    if (line == null) break
//                                    println("Received: $line")
                                    val datagramValue = readChannel.readByteArray(4096)
                                    onDatagramReceived(datagramValue, TipoPacote.RAW)
                                } catch (ex: Exception) {
                                    println(ex.message)
                                }
                            }
                        } finally {
                            socket.close()
                            onSocketConnected(false)
                        }
                    }
                }

            } catch (ex: Exception) {
                onSocketConnected(false)
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
                onSocketConnected(true)
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

////                                // writeChannel.writeStringUtf8("Hello from Server!")
////                                delay(200)
////                            }
//                        } finally {
//                            socket.close()
//                        }
//                    }
                }
            } catch (ex: Exception) {
                onSocketConnected(false)
                println(ex.message)
            }
        }
        // myJob?.start()
    }


    fun stop() {
        myJob?.cancel()
        serverSocket?.close()
        onSocketConnected(false)

    }


}