package io.github.jonintendo.connection.socketkmp.server

import io.github.jonintendo.connection.socketkmp.SocketKMP
import io.github.jonintendo.connection.socketkmp.TipoPacote
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.ServerSocket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.writeByteArray
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex

class ServerSocketTCP(
    val port: Int
) : SocketKMP("", port) {

    var serverSocket: ServerSocket? = null

    fun start(tipo: TipoPacote = TipoPacote.RAW) {
        reading = true
        errorCount = 0

        myJob = customScope.launch {
            try {
                val selectorManager = SelectorManager(Dispatchers.IO)
                serverSocket = aSocket(selectorManager).tcp().bind("0.0.0.0", port)
                onSocketConnected(true)
                println("Server is listening at ${serverSocket!!.localAddress}")

                while (true) {
                    val socket = serverSocket!!.accept()
                    try {

                        val writing = launch {
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


                        launch {
                            val readChannel = socket.openReadChannel()
                            while (reading) {
                                try {
//                                    val line = readChannel.readUTF8Line()
//                                    if (line == null) break
//                                    println("Received: $line")
                                    val datagramValue = readChannel.readByteArray(4096)
                                    onDatagramReceived(datagramValue, TipoPacote.RAW)
                                } catch (ex: Exception) {
                                    if (errorCount > 5) {
                                        writing.cancel()
                                        reading = false
                                    }
                                    else {
                                        errorCount++
                                        delay(1000)
                                    }
                                    println(ex.message)
                                }
                            }

                        }

                    } finally {
                      //  socket.close()
                      //  onSocketConnected(false)
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
        reading = false
        myJob?.cancel()
        serverSocket?.close()
        onSocketConnected(false)

    }


}