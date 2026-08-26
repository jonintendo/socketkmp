package io.github.jonintendo.connection.socketkmp

import io.github.jonintendo.connection.socketkmp.SocketListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.update
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

open class SocketKMP(
    val serverip: String,
    val serverport: Int,
) {
    protected val lastState = MutableStateFlow<SocketProperties>(SocketProperties())
    val lastStateFlow: SharedFlow<SocketProperties> = lastState


    protected var listeners = mutableListOf<SocketListener>()
    fun addListener(listener: SocketListener) {
        listeners.add(listener)
    }

    fun removeListener(listener: SocketListener) {
        listeners.remove(listener)
    }

    @OptIn(ExperimentalTime::class)
    protected fun onDatagramReceived(datagram: ByteArray, tipoPacote: TipoPacote) {
        lastState.update { it.copy(lastDatagramData = datagram, lastDatagramType = tipoPacote, lastDatagramTime = Clock.System.now().epochSeconds ) }
        listeners.forEach { listener ->
            listener.onDatagramReceived(datagram, tipoPacote, serverip, serverport)
        }
    }


    protected fun onSocketConnected(connected: Boolean) {
        lastState.update { it.copy(lastConnectionState = connected) }
        listeners.forEach { listener ->
            listener.onSocketConnected(connected, serverip, serverport)
        }
    }

    protected fun onError(msg: String) {
        lastState.update { it.copy(lastError = msg) }
        listeners.forEach { listener ->
            listener.onError(msg, serverip, serverport)
        }
    }


    protected var byteArraySocketFlow = MutableSharedFlow<ByteArray>(
        extraBufferCapacity = 1
    )

    fun send(byteArray: ByteArray){
        byteArraySocketFlow.tryEmit(byteArray)
    }


    protected var myJob: Job? = null
    val customScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    protected var reading = false
    protected var errorCount = 0
}