package com.connection.socket

interface SocketListener {
    fun onDatagramReceived(data: ByteArray)
}