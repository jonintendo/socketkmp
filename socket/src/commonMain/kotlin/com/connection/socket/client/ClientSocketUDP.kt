package com.connection.socket.client


import com.connection.socket.FrameSocket
import com.connection.socket.SocketListener
import com.connection.socket.TipoPacote
import com.connection.socket.byteArrayToIntLittleEndian
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.BoundDatagramSocket
import io.ktor.network.sockets.Datagram
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.append
import io.ktor.utils.io.core.build
import io.ktor.utils.io.core.toByteArray
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.IO
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import kotlinx.io.readByteArray
import kotlin.collections.plus

class ClientSocketUDP(
    private val serverip: String,
    private val serverport: Int,
) {

    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    var serverSocket: BoundDatagramSocket? = null
    var processing = false


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

    companion object {
        val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        fun sendUDP(ip: String, port: Int, message: String) = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                val socket = aSocket(selectorManager)
                    .udp()
                    //.connect(InetSocketAddress("192.168.0.6", 50100))
                    .connect(InetSocketAddress(ip, port))

                socket.outgoing.send(
                    Datagram(
                        ByteReadPacket(message.toByteArray()),
                        InetSocketAddress(ip, port)
                    )
                )
                println("Message sent.")
            } catch (ex: Exception) {
                println(ex.message)
            }
        }
    }

    fun start() {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                val socket = aSocket(selectorManager)
                    .udp()
                    //.connect(InetSocketAddress("192.168.0.6", 50100))
                    .connect(InetSocketAddress(serverip, serverport))


                datagramSocketFlow.collect { datagram ->

                    if (processing) return@collect
                    processing = true

                    //println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${datagram.size}")
                    println("${datagram.valor} socketttttttttttttttttttttttttttttttttt")

                    socket.outgoing.send(
                        Datagram(
                            ByteReadPacket(datagram.tamanho),
                            InetSocketAddress(serverip, serverport)
                        )
                    )

                    val chunkSize = 4096
                    val byteArrays: List<ByteArray> =
                        datagram.valor.asList().chunked(chunkSize) { it.toByteArray() }
                    byteArrays.forEach {
                        // println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${it.size}")
                        socket.outgoing.send(
                            Datagram(
                                ByteReadPacket(it),
                                InetSocketAddress(serverip, serverport)
                            )
                        )
                    }
                    processing = false
                }

            } catch (ex: Exception) {
                println(ex.stackTraceToString())
            }
        }
    }


    fun startForReceive(tipo: TipoPacote) {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)

                val socket = aSocket(selectorManager)
                    .udp()
                    //.connect(InetSocketAddress("192.168.0.6", 50100))
                    .connect(InetSocketAddress(serverip, serverport))
                println("conectado com $serverip, $serverport")
                socket.outgoing.send(
                    Datagram(
                        packet = BytePacketBuilder().apply { append("Hello from Client!") }
                            .build(),
                        address = InetSocketAddress(
                            serverip,
                            serverport
                        ) // Destination address from the received packet
                    )
                )


                try {
                    for (datagram in socket.incoming) {
                        // Extracting IP and Port from the datagram address
                        val address = datagram.address as? InetSocketAddress
                        val senderIp = address?.hostname
                        val senderPort = address?.port

                        when (tipo) {
                            TipoPacote.RAW -> {
                                val datagramValue = datagram.packet.readByteArray()
                                onDatagramReceived(datagramValue)
                                lastDatagramData.value = datagramValue
                            }

                            TipoPacote.FRAME -> {
                                val datagramValue = datagram.packet.readByteArray()
                                processReceivedFrameDatagramUDP(datagramValue)
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
    }

    var frameSize = 0
    var messageFromSocket: ByteArray = byteArrayOf()

    fun processReceivedFrameDatagramUDP(frameDatagram: ByteArray) {
        //println(frameDatagram.size)
        if (frameDatagram.size == 4) {
            frameSize = byteArrayToIntLittleEndian(frameDatagram)
            println("size do agregado ${messageFromSocket.size}   size recebido $frameSize")
            println("$messageFromSocket socketttttttttttttttttttttttttttttttttt")
            onDatagramReceived(messageFromSocket)
            lastDatagramData.value = messageFromSocket
            println("sizeeeeeeeeeeeeeeeeeeeeeeeee ${messageFromSocket.size}")
            messageFromSocket = byteArrayOf()

        } else {
            messageFromSocket += frameDatagram
            println(messageFromSocket)
        }
    }

    fun stop() {
        myJob?.cancel()
    }

}