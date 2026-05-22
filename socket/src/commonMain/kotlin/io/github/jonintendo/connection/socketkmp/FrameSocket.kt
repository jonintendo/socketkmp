package io.github.jonintendo.connection.socketkmp

data class FrameSocket(
    val tamanho: ByteArray,
    val valor: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as FrameSocket

        if (!tamanho.contentEquals(other.tamanho)) return false
        if (!valor.contentEquals(other.valor)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tamanho.contentHashCode()
        result = 31 * result + valor.contentHashCode()
        return result
    }
}
