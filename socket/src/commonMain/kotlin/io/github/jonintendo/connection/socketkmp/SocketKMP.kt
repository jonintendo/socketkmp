package io.github.jonintendo.connection.socketkmp

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlin.time.ExperimentalTime

open class SocketKMP(
    val serverip: String,
    val serverport: Int,
) {
    protected val lastStatus = MutableStateFlow(SocketStatus(false))
    val lastStatusFlow = lastStatus.asStateFlow()

    protected val lastData = MutableStateFlow(SocketData(byteArrayOf(), TipoPacote.RAW))
    val lastDataFlow = lastData.asStateFlow()

    protected val lastNotification = MutableSharedFlow<SocketNotification>()
    val lastNotificationFlow = lastNotification.asSharedFlow()


    @OptIn(ExperimentalTime::class)
    protected fun onDatagramReceived(datagram: ByteArray, tipoPacote: TipoPacote) {
        lastData.update { SocketData(datagram, tipoPacote) }
    }


    protected fun onSocketConnected(connected: Boolean) {
        lastStatus.update { SocketStatus(connected) }
    }

    protected fun onError(msg: String) {
        lastNotification.tryEmit(SocketNotification(TypeNotification.Error, msg))
    }


    protected var byteArraySocketFlow = MutableSharedFlow<ByteArray>(
        extraBufferCapacity = 1
    )

    fun send(byteArray: ByteArray) {
        byteArraySocketFlow.tryEmit(byteArray)
    }


    protected var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    protected var reading = false
    protected var errorCount = 0
}