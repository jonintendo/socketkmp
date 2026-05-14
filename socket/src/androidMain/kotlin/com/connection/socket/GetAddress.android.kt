package com.connection.socket

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.net.Inet4Address
import java.net.NetworkInterface
import kotlin.collections.iterator

actual suspend fun getAddress(): String{
    var address = "No IP"
    val interfaces = withContext(Dispatchers.IO) {
        NetworkInterface.getNetworkInterfaces()
    }

    for (iface in interfaces) {
        for (addr in iface.inetAddresses) {
            if (!addr.isLoopbackAddress && addr is Inet4Address) {
                address = addr.hostAddress as String
                return address
            }
        }
    }

    return address
}