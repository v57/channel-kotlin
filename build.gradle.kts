import org.gradle.testing.jacoco.tasks.JacocoReport
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.io.File

plugins {
  kotlin("jvm") version "2.2.21"
  jacoco
}

repositories {
  mavenCentral()
}

kotlin {
  jvmToolchain(21)
  compilerOptions {
    jvmTarget.set(JvmTarget.JVM_21)
  }
}

val benchmarkSourceSet = sourceSets.create("benchmark") {
  java.srcDir("src/benchmark/kotlin")
  resources.srcDir("src/benchmark/resources")
}

configurations[benchmarkSourceSet.implementationConfigurationName].extendsFrom(configurations["implementation"])
configurations[benchmarkSourceSet.compileOnlyConfigurationName].extendsFrom(configurations["compileOnly"])
configurations[benchmarkSourceSet.runtimeOnlyConfigurationName].extendsFrom(configurations["runtimeOnly"])

benchmarkSourceSet.compileClasspath += sourceSets["main"].output + configurations[benchmarkSourceSet.compileClasspathConfigurationName]
benchmarkSourceSet.runtimeClasspath += benchmarkSourceSet.output + benchmarkSourceSet.compileClasspath + configurations[benchmarkSourceSet.runtimeClasspathConfigurationName]

dependencies {
  implementation("com.dslplatform:dsl-json-java8:1.10.0")
  implementation(kotlin("stdlib"))
  implementation(kotlin("reflect"))

  testImplementation(kotlin("test"))
  testImplementation(kotlin("test-junit5"))
}

tasks.withType<KotlinCompile>().configureEach {
  compilerOptions {
    jvmTarget.set(JvmTarget.JVM_21)
    freeCompilerArgs.add("-Xjsr305=strict")
  }
}

tasks.test {
  useJUnitPlatform()
  testLogging {
    events("failed", "skipped")
  }
}

val benchmarkClasses = tasks.named(benchmarkSourceSet.classesTaskName)

tasks.register<JavaExec>("runBenchmark") {
  group = "verification"
  description = "Run channel-kotlin benchmarks"
  dependsOn(benchmarkClasses)
  classpath = benchmarkSourceSet.runtimeClasspath
  mainClass.set("channel.benchmark.BenchmarkMainKt")
  environment("JAVA_HOME", "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home")
}

tasks.register<JavaExec>("runBenchmarkServer") {
  group = "verification"
  description = "Run a single Kotlin benchmark server process"
  dependsOn(benchmarkClasses)
  classpath = benchmarkSourceSet.runtimeClasspath
  mainClass.set("channel.benchmark.ServerProcessMainKt")
  environment("JAVA_HOME", "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home")
}

tasks.register<JavaExec>("runBenchmarkClient") {
  group = "verification"
  description = "Run a single Kotlin benchmark client process"
  dependsOn(benchmarkClasses)
  classpath = benchmarkSourceSet.runtimeClasspath
  mainClass.set("channel.benchmark.ClientProcessMainKt")
  environment("JAVA_HOME", "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home")
}

tasks.register<JavaExec>("runBenchmarkProcess") {
  group = "verification"
  description = "Run a single Kotlin process benchmark worker"
  dependsOn(benchmarkClasses)
  classpath = benchmarkSourceSet.runtimeClasspath
  mainClass.set("channel.benchmark.ProcessBenchmarkMainKt")
  environment("JAVA_HOME", "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home")
}

tasks.register("writeBenchmarkClasspath") {
  group = "verification"
  description = "Write Kotlin benchmark runtime classpath for external launchers"
  dependsOn(benchmarkClasses)

  doLast {
    val classpath = benchmarkSourceSet.runtimeClasspath.files.joinToString(File.pathSeparator) { it.absolutePath }
    val outputDir = layout.buildDirectory.dir("benchmark").get().asFile
    outputDir.mkdirs()
    outputDir.resolve("classpath.txt").writeText(classpath)
  }
}

tasks.register("jvmTest") {
  group = "verification"
  description = "Compatibility alias for the JVM test suite"
  dependsOn(tasks.test)
}

jacoco {
  toolVersion = "0.8.12"
}

tasks.register<JacocoReport>("jacocoJvmReport") {
  group = "verification"
  description = "Generate JaCoCo coverage report for test"
  dependsOn(tasks.test)

  executionData.setFrom(fileTree(layout.buildDirectory) {
    include("jacoco/*.exec", "**/*.exec")
  })

  sourceDirectories.setFrom(files("src/main/kotlin"))
  classDirectories.setFrom(fileTree(layout.buildDirectory.dir("classes/kotlin/main")))

  reports {
    xml.required.set(true)
    xml.outputLocation.set(layout.buildDirectory.file("reports/jacoco/jacocoJvmReport/jacocoJvmReport.xml"))
    html.required.set(true)
    csv.required.set(false)
  }

  doFirst {
    val execFiles = executionData.files.filter { it.exists() }
    if (execFiles.isEmpty()) {
      throw GradleException("No JaCoCo execution data found. Run test first.")
    }
  }
}
