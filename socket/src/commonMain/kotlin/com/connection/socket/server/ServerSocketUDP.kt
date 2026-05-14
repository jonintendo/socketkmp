package com.connection.socket.server

import com.connection.socket.FrameSocket
import com.connection.socket.SocketListener

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
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.io.InternalIoApi
import kotlinx.io.readByteArray

class ServerSocketUDP(private val port: Int) {
    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    var serverSocket: BoundDatagramSocket? = null


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
                serverSocket = aSocket(selectorManager).udp().bind("0.0.0.0", port)

                println("Server is listening at ${serverSocket!!.localAddress}")
                try {
                    for (datagram in serverSocket!!.incoming) {

                        // Extracting IP and Port from the datagram address
                        val address = datagram.address as? InetSocketAddress
                        val senderIp = address?.hostname
                        val senderPort = address?.port

//                        val message = datagram.packet.readText()
//                        println("Received '$message' from $senderIp:$senderPort")
                        val datagramValue = datagram.packet.readByteArray()
                        onDatagramReceived(datagramValue)
                        lastDatagramData.value = datagramValue
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
                serverSocket = aSocket(selectorManager).udp().bind("0.0.0.0", port)
                var clientIp = ""
                var clientPort = 0
                println("Server is listening at ${serverSocket!!.localAddress}")
                try {

                    launch {
                        datagramSocketFlow.collect { datagram ->

                            if (processing || clientPort==0) return@collect
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
                        }
                    }

                    for (datagram in serverSocket!!.incoming) {
                        println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
                        val address = datagram.address as? InetSocketAddress
                        clientIp = address?.hostname ?: ""
                        clientPort = address?.port ?: 0

                        val datagramValue = datagram.packet.readByteArray()
                        onDatagramReceived(datagramValue)
                        lastDatagramData.value = datagramValue

                        serverSocket!!.outgoing.send(
                            Datagram(
                                packet = BytePacketBuilder().apply { append("Hello from Server!") }
                                    .build(),
                                address = datagram.address // Destination address from the received packet
                            )
                        )
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


    fun stop() {
        myJob?.cancel()
        serverSocket?.close()

    }


}