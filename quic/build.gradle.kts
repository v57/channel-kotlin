import org.jetbrains.kotlin.gradle.dsl.JvmTarget

// channel-kotlin-quic: the QUIC transport (kwik, pure Java). Depend on it only when you use quic://.
plugins {
  kotlin("jvm")
}

repositories {
  mavenCentral()
}

base {
  archivesName.set("channel-kotlin-quic")
}

kotlin {
  jvmToolchain(21)
  compilerOptions {
    jvmTarget.set(JvmTarget.JVM_21)
    freeCompilerArgs.addAll("-Xjsr305=strict", "-opt-in=channel.ChannelTransportApi")
  }
}

dependencies {
  api(rootProject)
  implementation("tech.kwik:kwik:0.12.1")

  testImplementation(kotlin("test"))
  testImplementation(kotlin("test-junit5"))
}

tasks.test {
  useJUnitPlatform()
  // The QUIC interop tests start their own TypeScript fixture on this port (UDP and TCP) unless one runs there.
  environment("CHANNEL_QUIC_TEST_PORT", System.getenv("CHANNEL_QUIC_TEST_PORT") ?: "2106")
  testLogging {
    events("failed", "skipped")
    exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
  }
}

tasks.register("jvmTest") {
  group = "verification"
  description = "Compatibility alias for the QUIC test suite"
  dependsOn(tasks.test)
}
