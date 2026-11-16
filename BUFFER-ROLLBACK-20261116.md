# Cofnięcie ograniczeń bufora z Arc 1.2.0

Zmiana na `rust_port`: `7a724f399`. Urządzenie zgłoszone przez użytkownika:
Homatics R 4K Plus; przygotowywany APK pozostaje `armeabi-v7a`.

## Ustalony zakres

Porównanie tagów upstream:

- `1.1.0-arc.4`: `2d30534d8aca2db93006dd36aade00e8c43b567e`.
- `1.2.0-arc.1`: `343b27591b65ca57372c2a7d420e767a8f86260c`.
- Commit wprowadzający wskazane ograniczenia: `91ed74d65e4b69e38c8be1ff7fae6d1a5c811e2a`.

Źródło: [wydanie 1.2.0-arc.1](https://github.com/ysosrs123/NuvioTV-Fork/releases/tag/1.2.0-arc.1)
i [commit zmieniający bufor](https://github.com/ysosrs123/NuvioTV-Fork/commit/91ed74d65e4b69e38c8be1ff7fae6d1a5c811e2a).

Usunięto:

- Narzucanie budżetowi ExoPlayera limitu na podstawie `VmSize` i progu 3300 MB.
  Działał również dla ręcznie ustawionego bufora i mógł obniżyć go do minimum 50 MB.
  `VmSize` jest rozmiarem wirtualnej przestrzeni procesu, nie pomiarem faktycznie
  zapełnionego bufora ani wolnego fizycznego RAM.
- Callback, który przy krytycznej presji wprowadzał nowy limit dwóch pobrań
  ParallelRange i wyrzucał dalsze fragmenty prefetch.
- Odpowiadające im pola diagnostyki i testy usuniętej polityki.

`BufferMemoryKeeper`, `BufferSizing`, `PlaybackMemoryFacts` i integracja keepra
w `PlayerRuntimeControllerBufferMemory` są identyczne z poprzednim tagiem.
Zachowano starsze reguły presji RAM, swapu, pauzy i pożyczania pamięci, które
istniały już w 1.1.0-arc.4. Nie cofnięto napraw ownership i anulowania żądań,
mniejszych stosów/puli workerów, kodeków ani pozostałych zmian wydania.
Budżet aktywnych bloków i puli Rust oraz wspólny ConnectionLedger pozostają.

## Test regresji

Przed cofnięciem uruchomiono test na dotychczasowym kodzie, z budżetem 1200 MB
i symulowanym `VmSize=3500 MB`. Otrzymano oczekiwany błąd:

```
expected: 1258291200 bytes (1200 MiB)
actual:     52428800 bytes (50 MiB)
```

Po cofnięciu test sprawdza osobno cele 800/1000/1200 MB, start i trzy seeki,
zachowanie celu alokatora oraz decyzję o kontynuacji przy 10 i 120 sekundach
bufora. Limit czasu 600 sekund nadal zatrzymuje pobieranie zgodnie z ustawieniem.
Test kontroluje decyzje polityki; nie alokuje 1200 MB ani nie udaje pomiaru boxa.

Po cofnięciu przeszło **670 testów w 80 zestawach**, bez błędów i pominięć.
W tym: 51 testów BufferMemoryKeeper, testy BufferSizing/LoadControl,
ParallelRange/AutoSpeedup/prewarm/limitów połączeń oraz updatera i streaming.
Dwa testy adaptera użyły prawdziwego hostowego JNI.

```powershell
.\gradlew.bat :app:testFullReleaseUnitTest --tests '*Buffer*' --tests '*LoadControl*' --tests '*ParallelRange*' --tests '*AutoSpeedup*' --tests '*PlaybackPrewarm*' --tests '*ConnectionLedger*' --tests '*HostSpeeds*' --tests '*streaming*' --tests '*updater*' -PrustHostJniDir=native/streaming-engine/target/debug --max-workers=1 '-Pkotlin.compiler.execution.strategy=in-process'
```

## Ręczna weryfikacja na Homatics

Użyć tej samej konfiguracji i materiału co przy błędzie, najpierw z backendem
Kotlin. Ustawić ręczny cel 1200 MB i pożądany czas bufora, odtworzyć fragment,
przewinąć kilka razy w obie strony i sprawdzić dalsze uzupełnianie. Następnie
można powtórzyć z Rust przy tych samych ustawieniach. Bufor może zatrzymać się
na celu czasowym przed zapełnieniem celu bajtowego; nie jest to nowy limit 10 s.

Rzeczywiste napełnienie, stabilność oraz pamięć na urządzeniu nie zostały zmierzone.
Użytkownik zadeklarował własną ręczną weryfikację.

Pakiet pozostaje `com.nuvio.tv.rusttest`, nazwa **Nuvio Rust Test**,
updater aplikacji wyłączony. Backend Kotlin jest nadal domyślny.

## Gotowy APK

`artifacts/Nuvio-Rust-Test-7a724f399-armeabi-v7a.apk`

115 738 973 bajtów; SHA-256:
`7ec5dde134037a9ea06790b137ab819c6f5309e092c2423e6fb6db5ab8fd17de`.
Release/R8 zbudowano dla ARM32 i ARM64. Oba przeszły weryfikator bibliotek
ELF/ZIP/JNI; ARMv7 ma zweryfikowany podpis v2. Silnik Rust ma te same hashe
jak przed cofnięciem. W DEX bazowego APK są ślady obu usuniętych regulatorów,
a w nowym ich nie ma. AAPT potwierdził osobny pakiet i nazwę Rust Test.

[CI dla 7a724f399](https://github.com/tomfle18/NuvioTV-Fork/actions/runs/43961164880):
core/JNI i Miri przeszły; Android jest jeszcze w toku w chwili zapisu raportu.
