package io.github.jonintendo.connection.socketkmp.client


import io.github.jonintendo.connection.socketkmp.server.SocketServerListener
import io.github.jonintendo.connection.socketkmp.SocketProperties
import io.github.jonintendo.connection.socketkmp.TipoPacote
import io.github.jonintendo.connection.socketkmp.byteArrayToIntLittleEndian
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Datagram
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.util.reflect.instanceOf
import io.ktor.utils.io.core.buildPacket
import io.ktor.utils.io.core.toByteArray
import io.ktor.utils.io.core.writeFully
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

class ClientSocketUDP(
    val serverip: String,
    val serverport: Int,
) {

    private var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())


    private val lastState = MutableStateFlow<SocketProperties>(SocketProperties())
    val lastStateFlow: SharedFlow<SocketProperties> = lastState


    private var listeners = mutableListOf<SocketClientListener>()
    fun addListener(listener: SocketClientListener) {
        listeners.add(listener)
    }

    fun removeListener(listener: SocketClientListener) {
        listeners.remove(listener)
    }

    @OptIn(ExperimentalTime::class)
    private fun onDatagramReceived(datagram: ByteArray, tipoPacote: TipoPacote) {
        lastState.update {
            it.copy(
                lastDatagramData = datagram,
                lastDatagramType = tipoPacote,
                lastDatagramTime = Clock.System.now().epochSeconds
            )
        }
        listeners.forEach { listener ->
            listener.onDatagramReceived(datagram, tipoPacote, serverip, serverport)
        }
    }

    private fun onSocketConnected(connected: Boolean) {
        lastState.update { it.copy(lastConnectionState = connected) }
        listeners.forEach { listener ->
            listener.onSocketConnected(connected, serverip, serverport)
        }
    }


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

                val messageBuffer = Buffer().apply { write(message.toByteArray()) }

                socket.outgoing.send(
                    Datagram(
                        messageBuffer,
                        InetSocketAddress(ip, port)
                    )
                )
                println("Message sent.")
            } catch (ex: Exception) {
                println(ex.message)
            }
        }
    }


    @OptIn(ExperimentalTime::class)
    fun start(tipo: TipoPacote = TipoPacote.RAW) {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                val socket = aSocket(selectorManager)
                    .udp()
                    .connect(InetSocketAddress(serverip, serverport))
                onSocketConnected(true)

                launch {
                    while (true) {
                        socket.outgoing.send(
                            Datagram(
                                Buffer().apply { write("oi".toByteArray()) },
                                InetSocketAddress(serverip, serverport)
                            )
                        )
                        if ((Clock.System.now().epochSeconds - lastState.value.lastDatagramTime) > 60)
                            onSocketConnected(false)

                        delay(1000)
                    }
                }

                launch {
                    byteArraySocketFlow.collect { datagram ->
                        println("${datagram} socketttttttttttttttttttttttttttttttttt")
                        try {
                            val chunkSize = 4096
                            val byteArrays: List<ByteArray> =
                                datagram.asList().chunked(chunkSize) { it.toByteArray() }
                            byteArrays.forEach {
                                socket.outgoing.send(
                                    Datagram(
                                        buildPacket {
                                            writeFully(it)
                                        },
                                        InetSocketAddress(serverip, serverport)
                                    )
                                )
                            }
                        } catch (ex: Exception) {
                            println("UDP in w${ex.message}")
                        }
                    }
                }

                launch {
                    for (datagram in socket.incoming) {
                        try {
                            val datagramValue = datagram.packet.readByteArray()

                            when (tipo) {
                                TipoPacote.RAW -> {
                                    onDatagramReceived(datagramValue, TipoPacote.RAW)
                                }

                                TipoPacote.FRAME -> {
                                    processReceivedFrameDatagramUDP(datagramValue)
                                }
                            }


                        } catch (e: CancellationException) {
                            throw e // Always rethrow cancellation exceptions!
                        } catch (ex: Exception) {
                            println("UDP in r${ex.message}")
                        }
                    }
                }

            } catch (e: CancellationException) {
                throw e // Always rethrow cancellation exceptions!
            } catch (ex: Exception) {
                onSocketConnected(false)
                println("UDP out ${ex.message}")
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