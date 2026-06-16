package io.github.jonintendo.connection.socketkmp

interface SocketListener {
    fun onSocketConnected(connected: Boolean, ip: String, port: Int)
    fun onDatagramReceived(data: ByteArray, tipoPacote: TipoPacote, ip: String, port: Int)
    fun onError(msg: String, ip: String, port: Int)
}