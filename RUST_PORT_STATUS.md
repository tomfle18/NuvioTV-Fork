# Port HTTP Rust: implementacja i walidacja

Data: 16 listopada 2026. Gałąź: `rust_port`.
Baza analizy: `main` `343b27591b65ca57372c2a7d420e767a8f86260c`.
Plan: `89362ce9c0a22c6fc655f5aa3aa0f7ba43e4930d`.

Port jest eksperymentalny. Kotlin pozostaje domyślny. Etapy 0-5 mają kod
i testy hosta; odbiór sprzętowy i etap 6 pozostają otwarte. Nie ma pomiaru
dowodzącego poprawy CPU, RAM, startu ani seek.

## Repozytorium i zachowany zakres

- Praca: `D:\projekty_codex\nuviotv`, istniejąca gałąź `rust_port`.
- Klon analizy `D:\projekty_codex\copycase_exsite\NuvioTV-Fork` był czysty
  i nie został zmodyfikowany. Kopię utworzono przez `git clone --no-hardlinks`.
- Nie resetowano gałęzi. Origin sprawdzono przed i po implementacji;
  nie zawierał dodatkowych commitów użytkownika.
- 12 lokalnych AAR ma niezmienione SHA-256 względem
  `native/streaming-engine/aar-baseline.json`.
- Media3/Arc, Dolby Vision, HDR, passthrough, FFmpeg, dav1d, libass,
  AFR/QMS, bufor próbek, torrent i MPV pozostały zachowane.
- HLS, DASH, źródła lokalne i niekwalifikowane HTTP pozostają przy Kotlinie.
  Nie dodano proxy, usług VPS ani publikacji wydania.

## Implementacja

`native/streaming-engine` zawiera HTTP/1.1 HTTPS/HTTP, Range, równoległość,
prefetch, backpressure, pulę bloków, sesje, niezależne kursory, adaptację
połączeń/Range, skończone retry i anulowanie. Obsługuje offsety 64-bit,
częściową publikację bloków i wznowienie body od brakującego bajtu.
Waliduje Content-Range, długość, encoding, duplikaty i ETag/Last-Modified.
Niespójność unieważnia sesję. Bez uczciwych validatorów serwera nie da się
wykryć podmiany pliku o tej samej długości; nie jest to kryptograficzna
weryfikacja materiału.

Niezmienny blok pozostaje żywy do zwolnienia ostatniego `Arc`.
Budżet obejmuje aktywne bufory i wolną pulę. Segment ma do 64 KiB,
alokacja dopasowuje się do fragmentu sieci, minimum 4 KiB. Limit globalny
buforów to 64 MiB; aplikacja wydziela do 16 MiB z istniejącego budżetu,
z rezerwą próbek/startu zależną od bitrate. Trim nadal liczy aktywne lease.
TLS, sockety i metadata nie należą do licznika pojemności bloków.

Kontrakt JNI 3 używa niepowtarzalnych uchwytów. `RustHttpDataSource`
obsługuje `byte[]` i `ByteBufferDataReader`. Direct buffer kopiuje pod
lease bloku, bez wystawiania jego pamięci JVM. `byte[]` ma scratch 16 KiB,
bez przypinania tablicy na czas sieci. Rust sam nie eliminuje kopii.
Sesja przeżywa close/open, a tokeny izolują spóźnione wyniki starego filmu.

`ConnectionLedger` atomowo przydziela każdy GET, retry, redirect i prewarm,
uwzględniając Kotlin, miniaturki i TorBox między hostami. `HostSpeeds`
zachowuje limity i historię. Prewarm 64 KiB należy do tej samej sesji;
cache dyskowy pozostaje nad nią. Prefetch wymaga 256 KiB ciągłego odczytu.
Przy pełnym buforze producenci czekają zamiast wyrzucać nieprzeczytane dane.

Jeden regulator Rust przenosi równania kosztu i główne reguły AutoSpeedup:
mierzone próby wzrostu, refill po seek/rebuffer, histerezę, redukcję po
odmowach i presji RAM. Kotlinowy regulator startuje dopiero po wyborze Kotlinu.
To jedna sesja single/multi zamiast kopiowania przełączeń DataSource;
pełna równoważność polityki w realnej sieci wymaga A/B.

Nieobsługiwane konfiguracje kierowane są do Kotlinu. Początkowy błąd
transportu pozwala na jeden fallback po zatrzymaniu starej sesji.
Po dostarczeniu bajtów stosowany jest jeden restart przez istniejące recovery
z zapisanej pozycji filmu. Nie sklejamy odpowiedzi backendów. Autoryzacja,
Changed i Protocol wracają do obsługi błędów aplikacji. Loader nie mnoży retry.
DNS używa resolvera Kotlin; blokującego resolvera nie można siłowo przerwać.
Czytelnik jest anulowany niezależnie, a workerów blocking jest maksymalnie 4.
Rustls/WebPKI nie odtwarza całego zaufania Androida. Proxy, mutable cookies,
custom trust/media servers i specjalne nagłówki korzystają z Kotlinu.
Nowy Set-Cookie w odpowiedzi lub przekierowaniu także wymusza kontrolowany
fallback, aby zachować własność magazynu cookies w Androidzie.

## Baza i narzędzia

- JDK 21.0.13+9, SDK 36, NDK 29.0.14206865, minSdk 24.
- Przypięte Rust 1.91.1, cargo-ndk 4.1.2 i oba targety ARM.
- Narzędzia są w `.tools`, bez zmiany systemowego PATH.
- Worktree `.tools/baseline` na `89362ce9c` zbudował fullDebug i fullRelease.
  Pierwsza próba z dwoma workerami zakończyła się OOM Kotlin. Jeden worker
  i kompilacja in-process przeszły. To ograniczenie builda, nie test transportu.
- Bazowe grupy JVM ParallelRange, AutoSpeedup, PlaybackPrewarm,
  ConnectionLedger i HostSpeeds przeszły przed zmianami.
- Użytkownik dostarczył `local.properties`. Jest ignorowany przez Git;
  nie zmieniano ani nie publikowano wartości. Końcowe lokalne APK używają tej
  konfiguracji. CI bez pliku używa wartości domyślnych.
- Testowy release jest podpisany kluczem debug przez istniejącą opcję
  `CI_USE_DEBUG_SIGNING=true`. Nie jest wydaniem produkcyjnym.

## Wyniki

| Warstwa | Wynik / zakres |
|---|---|
| Rust host | 13 unit + 17 transport przeszło; clippy `-D warnings`, fmt |
| Miri | 9 testów czystego rdzenia przeszło, nightly-2026-11-15 |
| JVM/JNI host Java | 1000 seek/reopen, 200 open/release races, byte[]/direct, bounds, handles, cancel, budżet, sloty; zero pozostawionych sesji/czytników/buforów |
| Weryfikator APK | 5 testów skryptu przeszło |
| Regresje JVM aplikacji | 435 testów w 39 zestawach, zero błędów i pominięć, w tym 2 testy adaptera z prawdziwym JNI hosta |
| APK debug i release/R8 | 4 APK z local.properties, oba ABI; ELF/ZIP/JNI przeszły, Rust identyczny w debug/release; R8 zachował nazwy mostu i callbacks |
| JNI na Androidzie | Nie wykonano: brak podłączonego urządzenia |
| Android TV, kodeki i HDMI | Nie wykonano; użytkownik potwierdził ręczną weryfikację na własnym boxie ARM32 |
| Kotlin/Rust A/B | Nie wykonano; nie deklarujemy poprawy wydajności |

Zdalny [przebieg CI dla końcowego kodu 7c54b7b47](https://github.com/tomfle18/NuvioTV-Fork/actions/runs/43949788562):
`core-jni` i `miri-core` przeszły. Job `android` w chwili zapisu raportu
nadal wykonuje testy JVM oraz budowanie debug/release. Lokalnie te testy,
oba ABI, R8 i kontrola wynikowych APK przeszły. Trwający przebieg nie jest
raportowany jako zakończony sukcesem.

Transport testuje >4 GiB, losowe odczyty, dwa kursory, 200/206/416,
błędny/duplikowany Range, encoding, urwany body, nieznaną długość, chunked,
redirect, usuwanie credentials między originami, zmianę zasobu, 429,
odmowę drugiego połączenia, timeout i brak slotu przez 11 sekund.
Fixture hosta sprawdza cel cancel <500 ms podczas nagłówków/body.
Miri nie sprawdza raw pointer JNI ani HTTP. Jest target cargo-fuzz
i deterministyczny smoke 100 tys. wejść parsera. Długiego fuzzingu
coverage-guided, Loom i sanitizerów JNI nie uruchomiono. Cel cargo-fuzz
przeszedł `cargo +nightly-2026-11-15 check --manifest-path native/streaming-engine/fuzz/Cargo.toml --bin range_store --locked`.

## APK i biblioteki

Główny APK dla boxa użytkownika: `armeabi-v7a`.

```
app/build/outputs/apk/full/release/app-full-armeabi-v7a-release.apk
app/build/outputs/apk/full/debug/app-full-armeabi-v7a-debug.apk
app/build/outputs/apk/full/release/app-full-arm64-v8a-release.apk
app/build/outputs/apk/full/debug/app-full-arm64-v8a-debug.apk
```

Końcowy release ARM32: 115 753 137 bajtów, SHA-256
`8475c1ff47c25f0c7161ca075cab76d092eadd3903b47a08d0a0c1a4c14a0c0d`.
ARM64: 126 512 195 bajtów, SHA-256
`a0fd19c69f78f5b865e6a2fa04382f363bfcbf0a3c3b53b523a4a86e1986af01`.
Aktualizacja istniejącej instalacji wymaga zgodnego podpisu; testowy podpis
debug może różnić się od podpisu zainstalowanego APK. Nie usuwać danych
aplikacji jako rutynowej części testu portu.

`verify_native_apk.py --require-rust` sprawdza ABI/ELF, biblioteki odtwarzania,
14 eksportów JNI Rust, ELF 16 KiB i wyrównanie nieskompresowanego Rust w ZIP.
Libass jest statycznie w `libasskt.so`; weryfikowane są zdefiniowane symbole.
Zastany ARM64 Conscrypt nie spełnia testu ELF 16 KiB także w APK bazowym.
ARM32 ma również wcześniejsze biblioteki 4 KiB. Nie potwierdzamy zgodności
całej aplikacji z urządzeniem 16 KiB ani działania API 24 samą kompilacją.

## Włączenie i rollback

Ustawienia odtwarzania, sekcja sieci: **Rust HTTP (eksperymentalny)**.
Włączenie działa od następnego odtwarzania progresywnego MKV/MP4.
Rzeczywisty backend i powód fallbacku są w ustawieniach i diagnostyce.
Log pamięci zawiera osobno network payload, delivered, allocated, free pool,
limit, live requests, request count, desired downloads, piece i peak requests.
To body HTTP, bez narzutu TCP/TLS na interfejsie sieciowym.

Powrót: wyłączyć przełącznik i rozpocząć odtwarzanie ponownie.
Opcja builda `-PwithRustStreaming=false` pomija nową bibliotekę; jej brak
wybiera Kotlin. Nie trzeba usuwać danych aplikacji.

## Powtórzenie testów

Wymagane cargo, cargo-ndk, JAVA_HOME i Android SDK/NDK na lokalnym środowisku.
Na tej maszynie narzędzia projektu włącza się dla bieżącego PowerShell:

```powershell
$env:CARGO_HOME="$PWD\.tools\cargo"
$env:RUSTUP_HOME="$PWD\.tools\rustup"
$env:PATH="$env:CARGO_HOME\bin;$env:PATH"
$env:JAVA_HOME='C:\Users\Tomasz\.jdks\jdk-21.0.13+9'
$env:ANDROID_HOME='C:\Users\Tomasz\AppData\Local\Android\Sdk'
```

Na nowej maszynie z rustup wykonać `cargo install cargo-ndk --version 4.1.2 --locked`;
plik rust-toolchain wybiera wersję kompilatora i oba targety.

```powershell
cargo fmt --manifest-path native/streaming-engine/Cargo.toml --check
cargo clippy --manifest-path native/streaming-engine/Cargo.toml --locked --all-targets -- -D warnings
cargo test --manifest-path native/streaming-engine/Cargo.toml --locked
python scripts/rust_jni_contract.py
cargo +nightly-2026-11-15 miri test --manifest-path native/streaming-engine/Cargo.toml --no-default-features --lib
python -m unittest discover -s scripts/tests -p test_verify_native_apk.py
.\gradlew.bat :app:testFullDebugUnitTest --tests '*ParallelRange*' --tests '*AutoSpeedup*' --tests '*PlaybackPrewarm*' --tests '*ConnectionLedger*' --tests '*HostSpeeds*' --tests '*SlowStart*' --tests '*streaming*' -PrustHostJniDir=native/streaming-engine/target/debug --max-workers=1
$env:CI_USE_DEBUG_SIGNING='true'
.\gradlew.bat :app:assembleFullDebug --max-workers=1 '-Pkotlin.compiler.execution.strategy=in-process'
.\gradlew.bat :app:assembleFullRelease --max-workers=1 '-Pkotlin.compiler.execution.strategy=in-process'
python scripts/verify_native_apk.py app/build/outputs/apk/full/release/app-full-armeabi-v7a-release.apk --abi armeabi-v7a --require-rust
```

Bez `rustHostJniDir` dwa testy adaptera JNI są jawnie pomijane. Workflow
`rust-streaming.yml` buduje bibliotekę hosta, naprawdę uruchamia te testy,
buduje oba ABI i R8. Nie publikuje wydania. Wynik zdalny odróżniamy od lokalnego.

## Pozostały odbiór

`scripts/rust_range_server.py` serwuje deterministyczny plik >4 GiB bez
trzymania go w RAM. Kontroluje delay, truncate, status i ignorowanie Range;
JSON podaje faktycznie wysłane body. Domyślnie loopback; można użyć adb reverse.

A/B wymaga tego samego release/R8, ABI, ustawień, budżetów, SHA-256 MKV/MP4,
cache i sieci. Minimum 30 naprzemiennych startów/seek dla backendu,
osobno cold/warm i cache on/off. Zapisać TTFF/seek p50/p95, rebuffer count/time,
CPU, PSS/RSS, Java/native memory, użyteczny/całkowity transfer i peak requests.
Sprawdzić MP4 faststart/moov na końcu, MKV, napisy/audio, miniaturki,
prewarm, następny odcinek, pause/resume, zmianę sieci, HDR/DV/passthrough,
AFR/QMS i 6 godzin bez rosnących zasobów. Bez tych wyników nie zmieniać
backendu domyślnego ani deklarować przewagi wydajnościowej.

## Commity implementacji

- `b8f601b80`: baza, inwentarz AAR i deterministyczny serwer Range.
- `28c257b77`: szkielet Rust/JNI, Gradle, oba ABI i podstawowy transport.
- `0f4cc0821`: scheduler, adaptacja, prefetch, budżet, sesje i testy wyścigów.
- `9c196d4d5`: adapter, ustawienia, pamięć, prewarm, lifecycle i recovery Kotlin.
- `194000141`: CI Rust/JNI/Android oraz weryfikator bibliotek APK.
- `b6fb005e7`: korekta konfiguracji SDK/nightly i przypięty runner CI.
- `7c54b7b47`: przekazanie nowych kontekstów cookies do Androida, testy odpowiedzi i redirect.

APK wskazuje commit kodu `7c54b7b47`; późniejszy commit raportu nie zmienia binariów.
