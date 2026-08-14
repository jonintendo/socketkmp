import java.io.FileInputStream
import java.util.Properties

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidKotlinMultiplatformLibrary)
    alias(libs.plugins.composeMultiplatform)
    alias(libs.plugins.composeCompiler)
    //alias(libs.plugins.ktor)
    alias(libs.plugins.maven.publish)
    signing
}



kotlin {


    androidLibrary {
        namespace = "io.github.jonintendo.connection.socketkmp"
        compileSdk = 36
        minSdk = 24


    }

    jvm()

    val xcfName = "socketKit"

    iosX64 {
        binaries.framework {
            baseName = xcfName
        }
    }

    iosArm64 {
        binaries.framework {
            baseName = xcfName
        }
    }

    iosSimulatorArm64 {
        binaries.framework {
            baseName = xcfName
        }
    }


    sourceSets {
        commonMain {
            dependencies {
                implementation(libs.ktor.network)
                implementation(libs.androidx.lifecycle.runtimeCompose)
            }
        }

        androidMain {
            dependencies {

            }
        }


        iosMain {
            dependencies {

            }
        }
        jvmMain {
            dependencies {

            }
        }
    }

}


group = "io.github.jonintendo"
version = "0.0.4"

mavenPublishing {

//    publishToMavenCentral()
//    signAllPublications()
    coordinates(
        group.toString(),
        "connection-socketkmp",
        version.toString()
    )

    pom {
        name = "Socket KMP"
        description = "A tile map component in pure compose multiplatform"
        inceptionYear = "2025"
        url = "https://github.com/jonintendo/socketkmp"

        licenses {
            license {
                name = "MIT License"
                url = "http://www.opensource.org/licenses/mit-license.php"
            }
        }

        developers {
            developer {
                name = "Jonathan Oliveira"
                email = "jonintendox@gmail.com"
            }
        }

        scm {
            url = "https://github.com/jonintendo/socketkmp"
            connection = "scm:git:git://github.com:jonintendo/socketkmp.git"
            developerConnection = "scm:git:ssh://github.com:jonintendo/socketkmp.git"
        }
    }
}

