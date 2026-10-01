package io.github.jonintendo.connection.socketkmp


import kotlinx.serialization.Serializable
//import kotlinx.serialization.builtins.serializer
//import kotlinx.serialization.json.JsonElement
//import kotlinx.serialization.json.JsonPrimitive
//import kotlinx.serialization.json.JsonTransformingSerializer


//object JsonAsStringSerializer : JsonTransformingSerializer<String>(String.serializer()) {
//    override fun transformDeserialize(element: JsonElement): JsonElement {
//        // Converts the raw JSON element (object, array, etc.) to a string primitive
//        return JsonPrimitive(element.toString())
//    }
//}

@Serializable
data class SocketNotification(var typeNotification: TypeNotification, var msg: String)

@Serializable
enum class TypeNotification() {
    Warning,
    Error,
    Result,
    Info
}

