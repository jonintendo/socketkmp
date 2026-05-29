package io.github.jonintendo.connection.socketkmp.server

import io.github.jonintendo.connection.socketkmp.SocketListener
import io.github.jonintendo.connection.socketkmp.TipoPacote

interface SocketServerListener : SocketListener {
    fun onSocketConnected(connected: Boolean, port: Int)
    fun onDatagramReceived(data: ByteArray, tipoPacote: TipoPacote, port: Int)
}