package io.github.jonintendo.connection.socketkmp

interface SocketListener {
    fun onDatagramReceived(data: ByteArray, tipoPacote: TipoPacote)
    fun onSocketConnected(connected: Boolean)
}