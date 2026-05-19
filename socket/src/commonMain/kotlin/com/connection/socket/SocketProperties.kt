package com.connection.socket

data class SocketProperties(
    var lastDatagramData: ByteArray = byteArrayOf(),
    var lastTipoPacote: TipoPacote = TipoPacote.RAW,
    var lastConnectionState: Boolean = false,
    var lasframeData: FrameSocket = FrameSocket(byteArrayOf(),byteArrayOf())
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as SocketProperties

        if (lastConnectionState != other.lastConnectionState) return false
        if (!lastDatagramData.contentEquals(other.lastDatagramData)) return false
        if (lasframeData != other.lasframeData) return false

        return true
    }

    override fun hashCode(): Int {
        var result = lastConnectionState.hashCode()
        result = 31 * result + lastDatagramData.contentHashCode()
        result = 31 * result + lasframeData.hashCode()
        return result
    }
}
