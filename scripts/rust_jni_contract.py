"""Build and run the real JNI contract on a host JVM (not an Android playback test)."""
import os
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
crate = root / "native/streaming-engine"
subprocess.run(["cargo", "build", "--locked"], cwd=crate, check=True)
target = Path(os.environ.get("CARGO_TARGET_DIR", crate / "target")).resolve()
output = target / "jni-tests"
output.mkdir(parents=True, exist_ok=True)
java_home = os.environ.get("JAVA_HOME")
java = str(Path(java_home) / "bin/java") if java_home else "java"
javac = str(Path(java_home) / "bin/javac") if java_home else "javac"
sources = sorted(str(p) for p in (crate / "tests/jni").rglob("*.java"))
subprocess.run([javac, "--add-modules", "jdk.httpserver", "-d", str(output), *sources], check=True)
library = "nuvio_streaming.dll" if os.name == "nt" else "libnuvio_streaming.dylib" if sys.platform == "darwin" else "libnuvio_streaming.so"
subprocess.run([java, "--add-modules", "jdk.httpserver", "-cp", str(output),
                "com.nuvio.tv.core.streaming.JniContractTest", str(target / "debug" / library)], check=True, timeout=180)
