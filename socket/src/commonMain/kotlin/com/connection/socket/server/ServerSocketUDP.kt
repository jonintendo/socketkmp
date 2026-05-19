package com.connection.socket.server

import com.connection.socket.FrameSocket
import com.connection.socket.SocketListener
import com.connection.socket.SocketProperties
import com.connection.socket.TipoPacote

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.BoundDatagramSocket
import io.ktor.network.sockets.Datagram
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.append
import io.ktor.utils.io.core.build
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
import kotlinx.io.readByteArray

class ServerSocketUDP(private val port: Int) {
    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    var serverSocket: BoundDatagramSocket? = null


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

    var byteArraySocketFlow = MutableSharedFlow<ByteArray>(
        extraBufferCapacity = 1
    )

    @OptIn(InternalIoApi::class)
    fun start() {
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
                                val chunkSize = 4096
                                val byteArrays: List<ByteArray> =
                                    datagram.asList().chunked(chunkSize) { it.toByteArray() }
                                byteArrays.forEach {
                                    // println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${it.size}")
                                    serverSocket!!.outgoing.send(
                                        Datagram(
                                            ByteReadPacket(it),
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

    val mutex = Mutex()
    var processing = false

    fun startForSend() {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                serverSocket = aSocket(selectorManager).udp().bind("0.0.0.0", port)
                var clientIp = ""
                var clientPort = 0
                onSocketConnected(true)
                println("Server is listening at ${serverSocket!!.localAddress}")

                launch {
                    datagramSocketFlow.collect { datagram ->
                        try {
                            if (processing || clientPort == 0) return@collect
                            processing = true

                            //println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${datagram.size}")
                            println("${datagram.valor} socketttttttttttttttttttttttttttttttttt")

                            serverSocket!!.outgoing.send(
                                Datagram(
                                    ByteReadPacket(datagram.tamanho),
                                    InetSocketAddress(clientIp, clientPort)
                                )
                            )

                            val chunkSize = 4096
                            val byteArrays: List<ByteArray> =
                                datagram.valor.asList().chunked(chunkSize) { it.toByteArray() }
                            byteArrays.forEach {
                                // println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${it.size}")
                                serverSocket!!.outgoing.send(
                                    Datagram(
                                        ByteReadPacket(it),
                                        InetSocketAddress(clientIp, clientPort)
                                    )
                                )
                            }
                            processing = false
                        } catch (ex: Exception) {
                            println(ex.message)
                        }
                    }
                }

                for (datagram in serverSocket!!.incoming) {
                    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")

                    try {
                        val address = datagram.address as? InetSocketAddress
                        clientIp = address?.hostname ?: ""
                        clientPort = address?.port ?: 0

                        val datagramValue = datagram.packet.readByteArray()
                        onDatagramReceived(datagramValue, TipoPacote.FRAME)

                        serverSocket!!.outgoing.send(
                            Datagram(
                                packet = BytePacketBuilder().apply { append("Hello from Server!") }
                                    .build(),
                                address = datagram.address // Destination address from the received packet
                            )
                        )

                    } catch (ex: Exception) {
                        println(ex.message)
                    }
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