package br.com.datastreambrasil.v4;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every way the JNI load can fail surfaces as the same "Failed to load both main and test
 * libraries", so the connector has to dig the distinguishing detail out of the cause chain.
 * The messages below were reproduced by running the shaded jar under Docker.
 */
class NativeLoadDiagnosisTest {

    private Throwable sdkFailure(String rootMessage) {
        var root = new UnsatisfiedLinkError(rootMessage);
        var wrapped = new RuntimeException("Failed to load both main and test libraries", root);
        return new NoClassDefFoundError("Could not initialize class "
                + "com.snowflake.ingest.streaming.FFIClient") {
            @Override
            public synchronized Throwable getCause() {
                return wrapped;
            }
        };
    }

    @Test
    void namesMuslAsTheCause() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(sdkFailure(
                "/tmp/cyclone_shared123/cyclone_shared.so: Error loading shared library "
                        + "libgcc_s.so.1: No such file or directory"));

        assertTrue(message.contains("Alpine/musl"), message);
        assertTrue(message.contains("glibc 2.26"), message);
    }

    @Test
    void namesNoexecTmpAsTheCause() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(sdkFailure(
                "/tmp/cyclone_shared123/cyclone_shared.so: failed to map segment from shared object"));

        assertTrue(message.contains("noexec"), message);
        assertTrue(message.contains("java.io.tmpdir"), message);
    }

    @Test
    void namesAReadOnlyRootAsTheCause() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(
                sdkFailure("/tmp/cyclone_shared123: Read-only file system"));

        assertTrue(message.contains("not writable"), message);
        assertTrue(message.contains("emptyDir"), message);
    }

    @Test
    void fallsBackToPointingAtTheLoaderLogWhenTheCauseIsUnfamiliar() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(sdkFailure("something new"));

        assertTrue(message.contains("FFIBootstrap"), message);
    }

    @Test
    void alwaysCarriesTheRootCauseAndTheEnvironment() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(
                sdkFailure("/tmp/x.so: failed to map segment from shared object"));

        // the detail that the SDK's own exception throws away
        assertTrue(message.contains("failed to map segment from shared object"), message);
        assertTrue(message.contains("java.io.tmpdir="), message);
        assertTrue(message.contains("arch="), message);
    }
}
