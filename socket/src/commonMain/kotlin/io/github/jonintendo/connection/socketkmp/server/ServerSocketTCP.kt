package io.github.jonintendo.connection.socketkmp.server

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

class ServerSocketTCP( val port: Int) {
    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    var serverSocket: ServerSocket? = null


    private val lastState = MutableStateFlow<SocketProperties>(SocketProperties())
    val lastStateFlow: SharedFlow<SocketProperties> = lastState


    private var listeners = mutableListOf<SocketServerListener>()
    fun addListener(listener: SocketServerListener) {
        listeners.add(listener)
    }

    fun removeListener(listener: SocketServerListener) {
        listeners.remove(listener)
    }

    private fun onDatagramReceived(datagram: ByteArray, tipoPacote: TipoPacote) {
        lastState.update { it.copy(lastDatagramData = datagram, lastTipoPacote = tipoPacote) }
        listeners.forEach { listener ->
            listener.onDatagramReceived(datagram, tipoPacote, port)
        }
    }

    private fun onSocketConnected(connected: Boolean) {
        lastState.update { it.copy(lastConnectionState = connected) }
        listeners.forEach { listener ->
            listener.onSocketConnected(connected, port)
        }
    }


    var byteArraySocketFlow = MutableSharedFlow<ByteArray>(
        extraBufferCapacity = 1
    )

    @OptIn(InternalIoApi::class)
    fun start(tipo: TipoPacote = TipoPacote.RAW) {
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


                    launch {
                        val writeChannel = socket.openWriteChannel(autoFlush = true)
                        byteArraySocketFlow.collect { datagram ->

                            if (processing) return@collect
                            processing = true

                            try {
                                //println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${datagram.size}")
                                println("${datagram} socketttttttttttttttttttttttttttttttttt")


                                if (tipo == TipoPacote.FRAME) {
                                    val bytes =
                                        ByteArray(4) { i -> (datagram.size shr (i * 8)).toByte() }
                                    writeChannel.writeByteArray(bytes)
                                }

                                val chunkSize = 4096
                                val byteArrays: List<ByteArray> =
                                    datagram.asList().chunked(chunkSize) { it.toByteArray() }
                                byteArrays.forEach {
                                    writeChannel.writeByteArray(it)
                                }
                            } catch (ex: Exception) {
                                println("TCP in w ${ex.message}")
                            }

                            processing = false
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


    fun stop() {
        myJob?.cancel()
        serverSocket?.close()
        onSocketConnected(false)

    }


}