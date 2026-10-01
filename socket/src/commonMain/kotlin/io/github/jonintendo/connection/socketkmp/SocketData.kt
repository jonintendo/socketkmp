package io.github.jonintendo.connection.socketkmp

data class SocketData(
    val data: ByteArray,
    val type: TipoPacote
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as SocketData

        if (!data.contentEquals(other.data)) return false
        if (type != other.type) return false

        return true
    }

    override fun hashCode(): Int {
        var result = data.contentHashCode()
        result = 31 * result + type.hashCode()
        return result
    }
}

enum class TipoPacote {
    RAW,
    FRAME
}