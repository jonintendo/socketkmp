package com.connection.socket

interface SocketListener {
    fun onDatagramReceived(data: ByteArray, tipoPacote: TipoPacote)
    fun onSocketConnected(connected: Boolean)
}