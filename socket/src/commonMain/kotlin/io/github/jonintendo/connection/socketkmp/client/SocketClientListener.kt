package io.github.jonintendo.connection.socketkmp.client

import io.github.jonintendo.connection.socketkmp.SocketListener
import io.github.jonintendo.connection.socketkmp.TipoPacote

interface SocketClientListener: SocketListener {
    fun onSocketConnected(connected: Boolean, ip: String, port: Int)
    fun onDatagramReceived(data: ByteArray, tipoPacote: TipoPacote, ip: String, port: Int)
}