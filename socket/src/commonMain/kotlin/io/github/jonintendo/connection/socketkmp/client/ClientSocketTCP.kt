package io.github.jonintendo.connection.socketkmp.client

import io.github.jonintendo.connection.socketkmp.SocketKMP
import io.github.jonintendo.connection.socketkmp.TipoPacote
import io.github.jonintendo.connection.socketkmp.byteArrayToIntLittleEndian
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.BoundDatagramSocket
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.core.toByteArray
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlin.coroutines.cancellation.CancellationException

class ClientSocketTCP(
     val ip: String,
     val port: Int,
) : SocketKMP(ip,port){

    var processing = false

    companion object {
        val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        fun sendTCP(ip: String, port: Int, message: String) = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                val socket = aSocket(selectorManager)
                    .tcp()
                    .connect(InetSocketAddress(ip, port))


                // 2. Open a write channel (autoFlush = true ensures data is sent immediately)
                val writeChannel = socket.openWriteChannel(autoFlush = true)
                try {
                    // 3. Send the data
                    val message = "Hello from Ktor client!\n"
                    writeChannel.writeStringUtf8(message)

                    writeChannel.writeByteArray(message.toByteArray())
                    println("Message sent.")
                } catch (ex: Exception) {
                    println(ex.message)
                } finally {
                    // 4. Clean up resources
                    socket.close()
                    selectorManager.close()
                }
            } catch (ex: Exception) {
                println(ex.message)
            }
        }
    }

    fun start(tipo: TipoPacote = TipoPacote.RAW) {
        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                val socket = aSocket(selectorManager)
                    .tcp()
                    .connect(InetSocketAddress(serverip, serverport))

                onSocketConnected(true)
                launch {
                    val writeChannel = socket.openWriteChannel(autoFlush = true)
                    byteArraySocketFlow.collect { datagram ->

                        if (processing) return@collect
                        processing = true

                        try {
                            //println("sizeeeeeeeeeeeeeeeeeeeeeeeeee  ${datagram.size}")
                            println("${datagram} socketttttttttttttttttttttttttttttttttt")


                            if (tipo == TipoPacote.FRAME) {
                                val bytes = ByteArray(4) { i -> (datagram.size shr (i * 8)).toByte() }
                                writeChannel.writeByteArray(bytes)
                            }

                            val chunkSize = 4096
                            val byteArrays: List<ByteArray> =
                                datagram.asList().chunked(chunkSize) { it.toByteArray() }
                            byteArrays.forEach {
                                writeChannel.writeByteArray(it)
                            }
                        } catch (ex: Exception) {
                            onError("TCP in w ${ex.message}")
                        }

                        processing = false
                    }
                }

                launch {
                    val readChannel = socket.openReadChannel()
                    //val writeChannel = socket.openWriteChannel(autoFlush = true)
                    try {
                        while (true) {
                            try {
                                //writeChannel.writeStringUtf8("Hello from Client!\n")
                                when (tipo) {
                                    TipoPacote.RAW -> {
                                        val datagramValue = readChannel.readByteArray(4096)
                                        onDatagramReceived(datagramValue, TipoPacote.RAW)
                                    }

                                    TipoPacote.FRAME -> {
                                        val datagramSize =
                                            byteArrayToIntLittleEndian(readChannel.readByteArray(4))

                                        val datagramValue = readChannel.readByteArray(datagramSize)
                                        onDatagramReceived(datagramValue, TipoPacote.FRAME)
                                        //processReceivedFrameDatagramTCP(datagramValue)
                                    }
                                }
                            } catch (e: CancellationException) {
                                throw e // Always rethrow cancellation exceptions!
                            } catch (ex: Exception) {
                                println("TCP in ${ex.message}")
                            }
                        }
                    } finally {
                        socket.close()
                        onSocketConnected(false)
                    }
                }

            } catch (ex: Exception) {
                onSocketConnected(false)
                println("TCP out ${ex.message}")
            }
        }
    }



    var frameSize = 0
    var messageFromSocket: ByteArray = byteArrayOf()
    var restMessageFromSocket: ByteArray = byteArrayOf()

    fun processReceivedFrameDatagramTCP(frameDatagram: ByteArray) {
        try {
            if (frameSize <= 0) {
                if (restMessageFromSocket.size > 0) {
                    frameSize =
                        byteArrayToIntLittleEndian(restMessageFromSocket.sliceArray(IntRange(0, 3)))
                    messageFromSocket += restMessageFromSocket.sliceArray(
                        IntRange(
                            4,
                            restMessageFromSocket.size - 1
                        )
                    )
                    messageFromSocket += frameDatagram
                    restMessageFromSocket = byteArrayOf()
                } else {
                    frameSize = byteArrayToIntLittleEndian(frameDatagram.sliceArray(IntRange(0, 3)))
                    messageFromSocket += restMessageFromSocket
                    messageFromSocket += frameDatagram.sliceArray(
                        IntRange(
                            4,
                            frameDatagram.size - 1
                        )
                    )
                    restMessageFromSocket = byteArrayOf()
                }

            } else {
                val howMuchToCompleteFrame = frameSize - messageFromSocket.size
                // println("sizeeeeeeeeeeeeeeeeeeeeeeeee ${frameSize}")

                if (howMuchToCompleteFrame >= frameDatagram.size) {
                    messageFromSocket += frameDatagram
                } else {
                    messageFromSocket += frameDatagram.sliceArray(
                        IntRange(
                            0,
                            howMuchToCompleteFrame - 1
                        )
                    )
                    restMessageFromSocket = frameDatagram.sliceArray(
                        IntRange(
                            howMuchToCompleteFrame,
                            frameDatagram.size - 1
                        )
                    )
                    println(messageFromSocket)
                }

                if (messageFromSocket.size == frameSize) {
                    println("size do agregado ${messageFromSocket.size}   size recebido $frameSize")
                    println("$messageFromSocket socketttttttttttttttttttttttttttttttttt")
                    onDatagramReceived(messageFromSocket, TipoPacote.FRAME)

                    messageFromSocket = byteArrayOf()
                    frameSize = 0
                }
            }
        } catch (ex: Exception) {
            println(ex.message)
        }
    }

    fun stop() {
        myJob?.cancel()
        onSocketConnected(false)
    }

}