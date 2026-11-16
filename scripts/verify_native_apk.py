"""Validate the fork's required native playback components before distributing an APK."""
import argparse
import hashlib
import json
import struct
import zipfile
from pathlib import Path

MACHINES = {"arm64-v8a": (2, 183), "armeabi-v7a": (1, 40)}
REQUIRED = {"libdovi_bridge.so", "libffmpegJNI.so", "libass.so", "libasskt.so"}
RUST_LIBRARY = "libnuvio_streaming.so"


def defined_symbols(data: bytes) -> set[str]:
    """Defined dynamic symbols, to distinguish static libass from an unresolved import."""
    try:
        wide = data[4] == 2
        offset = struct.unpack_from("<Q" if wide else "<I", data, 40 if wide else 32)[0]
        entry_size, count = struct.unpack_from("<HH", data, 58 if wide else 46)
        if entry_size < (64 if wide else 40) or offset + count * entry_size > len(data):
            return set()
        result = set()
        for i in range(count):
            section = offset + i * entry_size
            if struct.unpack_from("<I", data, section + 4)[0] != 11:
                continue
            start, size = struct.unpack_from("<QQ" if wide else "<II", data, section + (24 if wide else 16))
            link = struct.unpack_from("<I", data, section + (40 if wide else 24))[0]
            stride = struct.unpack_from("<Q" if wide else "<I", data, section + (56 if wide else 36))[0]
            if link >= count or stride < (24 if wide else 16) or start + size > len(data):
                return set()
            strings = offset + link * entry_size
            base, length = struct.unpack_from("<QQ" if wide else "<II", data, strings + (24 if wide else 16))
            if base + length > len(data):
                return set()
            for at in range(start, start + size - stride + 1, stride):
                index = struct.unpack_from("<I", data, at)[0]
                defined = struct.unpack_from("<H", data, at + (6 if wide else 14))[0]
                if defined and index < length:
                    end = data.find(b"\0", base + index, base + length)
                    if end >= 0:
                        result.add(data[base + index:end].decode("ascii", "replace"))
        return result
    except (IndexError, struct.error):
        return set()


def rust_alignment(data: bytes) -> None:
    """Validate every load segment, for ELF32 as well as ELF64, against 16 KiB pages."""
    if len(data) < 64 or data[:4] != b"\x7fELF" or data[4] not in (1, 2) or data[5] != 1:
        raise ValueError("Invalid ELF program header encoding")
    if data[4] == 2:
        offset = struct.unpack_from("<Q", data, 32)[0]
        entry_size, count = struct.unpack_from("<HH", data, 54)
        size = 56
    else:
        offset = struct.unpack_from("<I", data, 28)[0]
        entry_size, count = struct.unpack_from("<HH", data, 42)
        size = 32
    if not count or entry_size < size or offset + entry_size * count > len(data):
        raise ValueError("Invalid Rust ELF program headers")
    loads = 0
    for i in range(count):
        at = offset + entry_size * i
        if struct.unpack_from("<I", data, at)[0] != 1:
            continue
        loads += 1
        if data[4] == 2:
            file_offset, virtual = struct.unpack_from("<QQ", data, at + 8)
            alignment = struct.unpack_from("<Q", data, at + 48)[0]
        else:
            file_offset, virtual = struct.unpack_from("<II", data, at + 4)
            alignment = struct.unpack_from("<I", data, at + 28)[0]
        if alignment < 16384 or (file_offset - virtual) % 16384:
            raise ValueError("Rust ELF is not compatible with 16 KiB pages")
    if not loads:
        raise ValueError("Rust ELF has no load segments")


def verify(apk: Path, abi: str, require_rust: bool = False) -> dict:
    elf_class, machine = MACHINES[abi]
    with zipfile.ZipFile(apk) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate ZIP entries")
        corrupt = archive.testzip()
        if corrupt:
            raise ValueError(f"Corrupt ZIP entry: {corrupt}")
        libraries = [name for name in names if name.startswith("lib/") and name.endswith(".so")]
        required = REQUIRED | ({RUST_LIBRARY} if require_rust else set())
        missing = required - {name.rsplit("/", 1)[-1] for name in libraries if name.startswith(f"lib/{abi}/")}
        # The authoritative nuvio-libass AAR statically links libass into libasskt.so.
        # Accept it only when the renderer functions are defined, not merely named/imported.
        if "libass.so" in missing and f"lib/{abi}/libasskt.so" in libraries:
            symbols = defined_symbols(archive.read(f"lib/{abi}/libasskt.so"))
            if {"ass_library_init", "ass_renderer_init", "ass_render_frame"} <= symbols:
                missing.remove("libass.so")
        if missing:
            raise ValueError(f"Missing native playback libraries for {abi}: {sorted(missing)}")
        hashes = {}
        page16k_unsupported = []
        for name in libraries:
            if not name.startswith(f"lib/{abi}/"):
                raise ValueError(f"Unexpected ABI in single-ABI APK: {name}")
            data = archive.read(name)
            if len(data) < 20 or data[:4] != b"\x7fELF" or data[4] != elf_class or data[5] != 1:
                raise ValueError(f"Invalid ELF class/encoding: {name}")
            if struct.unpack_from("<H", data, 18)[0] != machine:
                raise ValueError(f"Wrong native architecture: {name}")
            try:
                rust_alignment(data)
            except ValueError:
                page16k_unsupported.append(name)
            if name.endswith("/" + RUST_LIBRARY):
                rust_alignment(data)
                expected = {"Java_com_nuvio_tv_core_streaming_RustStreamingBridge_" + method for method in
                            ("version", "resources", "create", "open", "awaitOpen", "readArray", "readDirect",
                             "cancel", "closeReader", "releaseSession", "updateLimit", "sample", "stats", "info")}
                absent = expected - defined_symbols(data)
                if absent:
                    raise ValueError(f"Rust JNI exports missing: {sorted(absent)}")
                entry = archive.getinfo(name)
                if entry.compress_type == zipfile.ZIP_STORED:
                    # AGP packages uncompressed native libraries for direct mmap by Android.
                    with apk.open("rb") as raw:
                        raw.seek(entry.header_offset + 26)
                        name_bytes, extra_bytes = struct.unpack("<HH", raw.read(4))
                    if (entry.header_offset + 30 + name_bytes + extra_bytes) % 16384:
                        raise ValueError("Rust library ZIP entry is not 16 KiB aligned")
            hashes[name] = hashlib.sha256(data).hexdigest()
        bridge = archive.read(f"lib/{abi}/libdovi_bridge.so")
        if b"dovi-bridge-libdovi-capi-0.2" not in bridge or b"dovi_convert_rpu_with_mode" not in bridge:
            raise ValueError("Dolby Vision bridge lacks real libdovi linkage")
    return {"apk": str(apk), "abi": abi, "sha256": hashlib.sha256(apk.read_bytes()).hexdigest(),
            "nativeLibraries": hashes, "elf16kUnsupported": page16k_unsupported}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("apk", type=Path)
    parser.add_argument("--abi", required=True, choices=MACHINES)
    parser.add_argument("--require-rust", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.apk, args.abi, args.require_rust), indent=2))
    except (ValueError, zipfile.BadZipFile, OSError) as error:
        parser.exit(1, f"Native APK validation failed: {error}\n")
