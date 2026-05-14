package com.connection.socket

fun byteArrayToIntBigEndian(bytes: ByteArray): Int {
    require(bytes.size >= 4) { "ByteArray must have at least 4 bytes" }
    return (bytes[0].toInt() and 0xFF shl 24) or
            (bytes[1].toInt() and 0xFF shl 16) or
            (bytes[2].toInt() and 0xFF shl 8) or
            (bytes[3].toInt() and 0xFF)
}


fun byteArrayToIntLittleEndian(bytes: ByteArray): Int {
    require(bytes.size >= 4) { "ByteArray must have at least 4 bytes" }
    return (bytes[0].toInt() and 0xFF) or
            (bytes[1].toInt() and 0xFF shl 8) or
            (bytes[2].toInt() and 0xFF shl 16) or
            (bytes[3].toInt() and 0xFF shl 24)
}
