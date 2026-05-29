package io.github.jonintendo.connection.socketkmp.server


import io.github.jonintendo.connection.socketkmp.server.SocketServerListener
import io.github.jonintendo.connection.socketkmp.SocketProperties
import io.github.jonintendo.connection.socketkmp.TipoPacote

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.BoundDatagramSocket
import io.ktor.network.sockets.Datagram
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.utils.io.core.buildPacket
import io.ktor.utils.io.core.writeFully

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
import kotlinx.io.Buffer
import kotlinx.io.readByteArray

class ServerSocketUDP(val port: Int) {
    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    var serverSocket: BoundDatagramSocket? = null


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

    val mutex = Mutex()

    fun start(tipo: TipoPacote = TipoPacote.RAW) {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                serverSocket = aSocket(selectorManager).udp().bind("0.0.0.0", port)
                var senderIp: String? = null
                var senderPort: Int? = null
                onSocketConnected(true)
                println("Server is listening at ${serverSocket!!.localAddress}")

                launch {
                    for (datagram in serverSocket!!.incoming) {
                        try {
                            // Extracting IP and Port from the datagram address
                            val address = datagram.address as? InetSocketAddress
                            if (address != null) {
                                senderIp = address.hostname
                                senderPort = address.port
                            }

                            val datagramValue = datagram.packet.readByteArray()
                            onDatagramReceived(datagramValue, TipoPacote.RAW)
                        } catch (ex: Exception) {
                            println(ex.message)
                        }
                    }
                }

                launch {
                    byteArraySocketFlow.collect { datagram ->
                        if (senderIp != null && senderPort != null) {
                            try {
                                println("${datagram} socketttttttttttttttttttttttttttttttttt")

                                if (tipo == TipoPacote.FRAME) {
                                    val bytes =
                                        ByteArray(4) { i -> (datagram.size shr (i * 8)).toByte() }
                                    val bytesBuffer = Buffer().apply { write(bytes) }

                                    serverSocket!!.outgoing.send(
                                        Datagram(
                                            bytesBuffer,
                                            InetSocketAddress(senderIp, senderPort)
                                        )
                                    )
                                }

                                val chunkSize = 4096
                                val byteArrays: List<ByteArray> =
                                    datagram.asList().chunked(chunkSize) { it.toByteArray() }
                                byteArrays.forEach {
                                    // println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${it.size}")
                                    serverSocket!!.outgoing.send(
                                        Datagram(
                                            buildPacket {
                                                writeFully(it)
                                            },
                                            InetSocketAddress(senderIp, senderPort)
                                        )
                                    )
                                }
                            } catch (ex: Exception) {
                                println(ex.message)
                            }
                        } else {
                            println("sem Cliente conectado socketttttttttttttttttttttttttttttttttt")
                        }
                    }
                }


            } catch (ex: Exception) {
                println(ex.message)
                onSocketConnected(false)
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