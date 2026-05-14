plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.androidKotlinMultiplatformLibrary)
    alias(libs.plugins.composeMultiplatform)
    alias(libs.plugins.composeCompiler)
    //alias(libs.plugins.ktor)
    alias(libs.plugins.maven.publish)
}

kotlin {

    // Target declarations - add or remove as needed below. These define
    // which platforms this KMP module supports.
    // See: https://kotlinlang.org/docs/multiplatform-discover-project.html#targets
    androidLibrary {
        namespace = "com.connection.socket"
        compileSdk = 36
        minSdk = 24

        withHostTestBuilder {
        }

        withDeviceTestBuilder {
            sourceSetTreeName = "test"
        }.configure {
            instrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        }
    }

    jvm()
    // For iOS targets, this is also where you should
    // configure native binary output. For more information, see:
    // https://kotlinlang.org/docs/multiplatform-build-native-binaries.html#build-xcframeworks

    // A step-by-step guide on how to include this library in an XCode
    // project can be found here:
    // https://developer.android.com/kotlin/multiplatform/migrate
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

    // Source set declarations.
    // Declaring a target automatically creates a source set with the same name. By default, the
    // Kotlin Gradle Plugin creates additional source sets that depend on each other, since it is
    // common to share sources between related targets.
    // See: https://kotlinlang.org/docs/multiplatform-hierarchy.html
    sourceSets {
        commonMain {
            dependencies {
                implementation(libs.ktor.network)
                implementation(libs.androidx.lifecycle.runtimeCompose)
            }
        }

        commonTest {
            dependencies {
                implementation(libs.kotlin.test)
            }
        }

        androidMain {
            dependencies {

            }
        }

        getByName("androidDeviceTest") {
            dependencies {
                implementation(libs.androidx.runner)
                implementation(libs.androidx.core)
                implementation(libs.androidx.testExt.junit)
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


group = "com.jonintendo"
version = "0.0.1"

mavenPublishing {
    //publishToMavenCentral()
    //signAllPublications()

    coordinates(
        group.toString(),
        "socket",
        version.toString()
    )

    pom {
        name = "socket"
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

