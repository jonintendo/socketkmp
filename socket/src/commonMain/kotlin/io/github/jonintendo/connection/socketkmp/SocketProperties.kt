package io.github.jonintendo.connection.socketkmp

import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

data class SocketProperties(
    var lastDatagramData: ByteArray = byteArrayOf(),
    var lastDatagramType: TipoPacote = TipoPacote.RAW,
    var lastConnectionState: Boolean = false,
    var lastDatagramTime: Long = 0,
    var lastError: String = ""
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as SocketProperties

        if (lastConnectionState != other.lastConnectionState) return false
        if (!lastDatagramData.contentEquals(other.lastDatagramData)) return false


        return true
    }

    override fun hashCode(): Int {
        var result = lastConnectionState.hashCode()
        result = 31 * result + lastDatagramData.contentHashCode()
        return result
    }
}
