package com.connection.socket.client


import com.connection.socket.FrameSocket
import com.connection.socket.SocketListener
import com.connection.socket.SocketProperties
import com.connection.socket.TipoPacote
import com.connection.socket.byteArrayToIntLittleEndian
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.BoundDatagramSocket
import io.ktor.network.sockets.Datagram
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.toByteArray
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.io.readByteArray

class ClientSocketUDP(
    private val serverip: String,
    private val serverport: Int,
) {

    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())


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
                    .connect(InetSocketAddress(serverip, serverport))
                onSocketConnected(true)
                launch {
                    byteArraySocketFlow.collect { datagram ->
                        println("${datagram} socketttttttttttttttttttttttttttttttttt")
                        try {
                            val chunkSize = 4096
                            val byteArrays: List<ByteArray> =
                                datagram.asList().chunked(chunkSize) { it.toByteArray() }
                            byteArrays.forEach {
                                // println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${it.size}")
                                socket.outgoing.send(
                                    Datagram(
                                        ByteReadPacket(it),
                                        InetSocketAddress(serverip, serverport)
                                    )
                                )
                            }
                        } catch (ex: Exception) {
                            println(ex.message)
                        }
                    }
                }

                launch {
                    for (datagram in socket.incoming) {
                        try {
                            onDatagramReceived(datagram.packet.readByteArray(), TipoPacote.RAW)
//                        val text = datagram.packet.readText()
//                        println("Received from ${datagram.address}: $text")
//                        address = datagram.address
                        } catch (ex: Exception) {
                            println(ex.message)
                        }
                    }
                }

            } catch (ex: Exception) {
                onSocketConnected(false)
                println(ex.message)
            }
        }
    }


    fun startForReceive(tipo: TipoPacote) {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)

                val socket = aSocket(selectorManager)
                    .udp()
                    .connect(InetSocketAddress(serverip, serverport))
                onSocketConnected(true)
                println("conectado com $serverip, $serverport")

//                socket.outgoing.send(
//                    Datagram(
//                        packet = BytePacketBuilder().apply { append("Hello from Client!") }
//                            .build(),
//                        address = InetSocketAddress(
//                            serverip,
//                            serverport
//                        ) // Destination address from the received packet
//                    )
//                )


                for (datagram in socket.incoming) {
                    try {
                        // Extracting IP and Port from the datagram address
                        val address = datagram.address as? InetSocketAddress
                        val senderIp = address?.hostname
                        val senderPort = address?.port

                        when (tipo) {
                            TipoPacote.RAW -> {
                                val datagramValue = datagram.packet.readByteArray()
                                onDatagramReceived(datagramValue, TipoPacote.RAW)
                            }

                            TipoPacote.FRAME -> {
                                val datagramValue = datagram.packet.readByteArray()
                                processReceivedFrameDatagramUDP(datagramValue)
                            }
                        }
                    } catch (ex: Exception) {
                        println(ex.message)
                    }
                }

            } catch (ex: Exception) {
                onSocketConnected(false)
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
            onDatagramReceived(messageFromSocket, TipoPacote.FRAME)
            println("sizeeeeeeeeeeeeeeeeeeeeeeeee ${messageFromSocket.size}")
            messageFromSocket = byteArrayOf()

        } else {
            messageFromSocket += frameDatagram
            println(messageFromSocket)
        }
    }

    fun stop() {
        myJob?.cancel()
        onSocketConnected(false)
    }

}