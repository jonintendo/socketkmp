package io.github.jonintendo.connection.socketkmp.client


import io.github.jonintendo.connection.socketkmp.SocketKMP
import io.github.jonintendo.connection.socketkmp.TipoPacote
import io.github.jonintendo.connection.socketkmp.byteArrayToIntLittleEndian
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Datagram
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.utils.io.core.buildPacket
import io.ktor.utils.io.core.toByteArray
import io.ktor.utils.io.core.writeFully
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

class ClientSocketUDP(
    val ip: String,
    val port: Int,
) : SocketKMP(ip, port) {


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

    var lastServerMessageTime: Long = 0

    @OptIn(ExperimentalTime::class)
    fun start(tipo: TipoPacote = TipoPacote.RAW) {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                val socket = aSocket(selectorManager)
                    .udp()
                    .connect(InetSocketAddress(serverip, serverport))

                if (tipo == TipoPacote.RAW) {
                    launch {
                        while (true) {
                            try {
                                socket.outgoing.send(
                                    Datagram(
                                        Buffer().apply { write("oi".toByteArray()) },
                                        InetSocketAddress(serverip, serverport)
                                    )
                                )
//                            onSocketConnected(true)
//                            println("true")
                            } catch (ex: Exception) {
                                onError(ex.message ?: "Erro desconhecido")
                                //   onSocketConnected(false)
                            } finally {
                                println((Clock.System.now().epochSeconds - lastServerMessageTime))
                                println((Clock.System.now().epochSeconds))
                                println((lastServerMessageTime))
                                if ((Clock.System.now().epochSeconds - lastServerMessageTime) > 10)
                                    onSocketConnected(false)
                            }

                            delay(1000)
                        }
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
                            onError("UDP in w${ex.message}")
                        }
                    }
                }

                launch {
                    for (datagram in socket.incoming) {
                        try {
                            val datagramValue = datagram.packet.readByteArray()
                            onSocketConnected(true)
                            when (tipo) {
                                TipoPacote.RAW -> {
                                    if (datagramValue.decodeToString() == "oi") {
                                        println("Conectado ao server $serverip:$serverport")
                                        lastServerMessageTime = Clock.System.now().epochSeconds
                                    } else {
                                        onDatagramReceived(datagramValue, TipoPacote.RAW)
                                    }
                                }

                                TipoPacote.FRAME -> {
                                    processReceivedFrameDatagramUDP(datagramValue)
                                }
                            }


                        } catch (e: CancellationException) {
                            throw e // Always rethrow cancellation exceptions!
                        } catch (ex: Exception) {
                            onError("UDP in r${ex.message}")
                        }
                    }
                }

            } catch (e: CancellationException) {
                throw e // Always rethrow cancellation exceptions!
            } catch (ex: Exception) {
                onSocketConnected(false)
                onError("UDP out ${ex.message}")

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