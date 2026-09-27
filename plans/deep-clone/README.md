# DEEP CLONE z przyrostową synchronizacją

Status: plan (nic nie jest jeszcze zaimplementowane).

## 1. Cel

Komenda SQL, która kopiuje tabelę Delta (pliki danych + log) do innej lokalizacji,
również na inny system plików lub do innej chmury. Ponowne uruchomienie tej samej
komendy przenosi tylko zmiany, które zaszły w źródle od poprzedniej synchronizacji.
Pozycja synchronizacji (wersja źródła) jest zapisywana w tabeli docelowej.

Docelowe zastosowanie: synchronizacja tabel między chmurami.

```sql
-- pierwsze uruchomienie: pełna kopia
CREATE OR REPLACE TABLE target DEEP CLONE source [LOCATION 's3://...'];

-- kolejne uruchomienia: tylko różnica od ostatniej zsynchronizowanej wersji
CREATE OR REPLACE TABLE target DEEP CLONE source;

-- time travel źródła też działa (synchronizacja do konkretnej wersji)
CREATE OR REPLACE TABLE target DEEP CLONE source VERSION AS OF 123;
```

Scala / Python API już mają parametr `isShallow` (`DeltaTable.clone(target, isShallow, ...)`,
w Pythonie domyślnie `isShallow=False`), który dziś rzuca `DELTA_UNSUPPORTED_DEEP_CLONE`.
Po zmianie `isShallow = false` uruchomi DEEP CLONE.

### Poza zakresem tego etapu

- Filtrowanie partycji (`WHERE`). To następny etap, opisany w sekcji 11.
- Odtwarzanie historii commit po commicie (1:1). Wszystkie zmiany z zakresu wersji
  trafiają do jednego commitu w tabeli docelowej.
- DEEP CLONE ze źródeł innych niż Delta (Parquet, Iceberg).
- Change Data Feed po stronie targetu, który odzwierciedlałby pojedyncze operacje źródła.

## 2. Stan obecny w kodzie

| Element | Plik | Uwagi |
|---|---|---|
| Gramatyka `SHALLOW CLONE` | `spark/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4:103` | Brak tokenu `DEEP`. |
| Parser | `spark/src/main/scala/io/delta/sql/parser/DeltaSqlParser.scala` (`visitClone`) | Tworzy `CloneTableStatement`. |
| Plan logiczny | `spark/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/CloneTableStatement.scala` | Brak informacji shallow/deep. |
| Time travel źródła | `spark/src/main/scala/org/apache/spark/sql/delta/PreprocessTimeTravel.scala` | Pattern match pozycyjny na `CloneTableStatement`. |
| Rozwiązanie komendy | `spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala` (`resolveCloneCommand`) | 5 miejsc tworzących `CloneTableCommand`, owiniętych w `CreateDeltaTableCommand`. |
| Komenda | `spark/src/main/scala/org/apache/spark/sql/delta/commands/CloneTableCommand.scala` | `CloneDeltaSource`, metryki wyjściowe. |
| Logika transakcji | `spark/src/main/scala/org/apache/spark/sql/delta/commands/CloneTableBase.scala` (`handleClone`) | Rzuca `cloneReplaceNonEmptyTable`, gdy target ma pliki. `handleNewDataFiles` absolutyzuje ścieżki (shallow). |
| Wywołanie w CREATE | `spark/src/main/scala/org/apache/spark/sql/delta/commands/CreateDeltaTableCommand.scala:198` | `cmd.handleClone(sparkSession, txn, deltaLog, metrics)`. |
| API Scala | `spark/src/main/scala/io/delta/tables/execution/DeltaTableOperations.scala:136` | `if (!isShallow) throw unsupportedDeepCloneException()`. |
| Operacja w logu | `DeltaOperations.Clone(source, sourceVersion)` | Metryki `numCopiedFiles`, `copiedFilesSize`, `numRemovedFiles`, `removedFilesSize` już zdefiniowane w `DeltaOperationMetrics.CLONE`, ale dziś zawsze 0. |
| Odczyt zmian | `DeltaLog.getChanges(startVersion, endVersion, catalogTableOpt, failOnDataLoss)` | Zwraca `(version, Seq[Action])`. Przy `failOnDataLoss = false` może zwrócić lukę, jeśli JSON-y zostały wyczyszczone. |
| Zapis pozycji | `OptimisticTransaction.updateSetTransaction(appId, version, lastUpdate)`, `txn.txnVersion(appId)` | Mechanizm używany przez `DeltaSink` do idempotentnych zapisów. |
| Klucz pliku | `FileAction.toUniqueFileActionTuple(tableRoot, useObjectIdentity)` | `(pathAsUri, dvUniqueId)`. Ten sam klucz stosuje log replay. |
| Klucz jako kolumna | `DeletionVectorDescriptor.uniqueIdExpression(col, tableRoot, useObjectIdentity)` | Do diffu na Datasetach. |
| Testy | `CloneTableSuiteBase`, `CloneTableSQLSuite`, `CloneTableScalaSuite`, `CloneTableTestMixin`, `DeltaSqlParserSuite` | Mixin ma już `testAllClones(... isShallow)`, ale uruchamia tylko SHALLOW. `CloneTableScalaSuite` ma test "deep clone not supported yet". |

## 3. Kluczowe decyzje projektowe

1. **Składnia `DEEP CLONE` w istniejącej komendzie CLONE**, a nie osobna komenda.
   Semantyka zgodna z Databricks: ponowne `CREATE OR REPLACE ... DEEP CLONE` jest przyrostowe.
   Reużywamy obsługę metadanych, protokołu, domain metadata, column mapping, katalogu.
2. **Pozycja przez `SetTransaction`** w tym samym commicie co zmiany plików:
   `appId = "delta.deepClone.<sourceMetadata.id>"`, `version = <wersja źródła>`.
   Atomowość: albo są pliki i pozycja, albo nic.
   `sourceMetadata.id` w `appId` sprawia, że po skasowaniu i odtworzeniu źródła pod tą samą
   ścieżką nie ma pozycji, więc wykonuje się pełny diff.
3. **Dwa tryby wyznaczania zmian**, z jednym, wspólnym sposobem zatwierdzania:
   - *przyrostowy* (domyślny): replay commitów źródła `(lastSynced, sourceVersion]`,
   - *pełny diff* (fallback): porównanie całych stanów źródła i targetu.
4. **Najpierw kopiowanie plików, potem commit.** Pliki Delta są niezmienne, więc ponowienie
   po awarii jest bezpieczne. Osierocone pliki z nieudanej próby usunie `VACUUM` targetu.
5. **Zgniatanie zakresu wersji do jednego commitu** w targecie.
6. **Brak commitu, gdy nie ma nic do zrobienia** (źródło bez zmian): komenda jest wtedy no-op,
   historia targetu nie rośnie od pustych commitów.
7. **Ścieżki względne zachowane 1:1**: `AddFile.path` w targecie ma ten sam ciąg co w źródle,
   fizycznie plik leży pod `targetPath/<ta sama ścieżka względna>`.

## 4. Algorytm

Wejście: `sourceSnapshot` (wersja `S`, ewentualnie z time travel), `txn` na tabeli docelowej.

```
appId      = "delta.deepClone." + sourceSnapshot.metadata.id
lastSynced = txn.txnVersion(appId)      // -1, jeśli brak

1. metadata = determineTargetMetadata(...)       // bez zmian względem dzisiejszego clone
   txn.updateMetadata(metadata, ignoreDefaultProperties = true)
   protocol = determineTargetProtocol(...)

2. changes =
     if lastSynced < 0                      -> fullDiff()
     else if lastSynced == S                -> brak zmian plików
     else if lastSynced > S                 -> fullDiff()   // time travel wstecz
     else incremental(lastSynced + 1, S) getOrElse fullDiff()

3. walidacja changes.filesToAdd (sekcja 6)

4. jeśli filesToAdd i filesToRemove puste
      i lastSynced == S
      i metadata == txn.snapshot.metadata
      i protocol == txn.snapshot.protocol
   -> zwróć metryki (same zera), bez commitu

5. copyFiles(changes.filesToAdd)               // rozproszony job Spark

6. txn.commitLarge(
     adds    = filesToAdd.map(_.copy(dataChange = true, baseRowId = None,
                 defaultRowCommitVersion = None, backReference = None, amtPassthrough = None)),
     removes = filesToRemove (RemoveFile z pliku targetu, dataChange = true),
     domain metadata (DomainMetadataUtils.handleDomainMetadataForCloneTable),
     SetTransaction(appId, S, Some(now)),
     protocol,
     operation = Clone(source, S, isShallow = false),
     metryki)
```

### 4.1 Tryb przyrostowy

```
finalState = LinkedHashMap[UniqueFileActionTuple, Option[AddFile]]
touchedPaths = Set[String]   // surowe AddFile.path / RemoveFile.path
expected = lastSynced + 1

for (version, actions) <- sourceLog.getChanges(lastSynced + 1, S, catalogTableOpt, failOnDataLoss = false):
  if version != expected -> return None      // luka: log wyczyszczony, fallback na fullDiff
  expected += 1
  for a in actions:
    AddFile    -> finalState(key(a)) = Some(a); touchedPaths += a.path
    RemoveFile -> finalState(key(a)) = None;    touchedPaths += a.path
    reszta     -> ignorujemy (Metadata/Protocol/DomainMetadata bierzemy ze snapshotu S)
if expected != S + 1 -> return None

// Porównanie z faktycznym stanem targetu, tylko dla dotkniętych ścieżek:
targetTouched = txn.snapshot.allFiles join broadcast(touchedPaths) on path   -> collect
targetByKey   = targetTouched.map(f => key(f) -> f).toMap

filesToAdd    = finalState.collect { case (k, Some(add)) if !targetByKey.contains(k) => add }
filesToRemove = finalState.collect { case (k, None) if targetByKey.contains(k) => targetByKey(k).removeWithTimestamp() }
```

`key(a) = a.toUniqueFileActionTuple(sourceDataPath, useObjectIdentity = false)`.
Pętla bez `return` w lambdach (while po iteratorze), zgodnie ze stylem repozytorium.

Dlaczego porównanie z targetem, a nie tylko stan netto:
- plik dodany i usunięty w tym samym zakresie nie trafia ani do kopiowania, ani do `RemoveFile`,
- ponowne `AddFile` istniejącego pliku z `dataChange = false` (np. przeliczenie statystyk)
  nie powoduje ponownego kopiowania,
- `RESTORE` w źródle (ponowne dodanie wcześniej usuniętego pliku) działa poprawnie,
- aktualizacja DV (`remove(path, dv1) + add(path, dv2)`) daje usunięcie starego wpisu
  i dodanie nowego, bo DV jest częścią klucza.

### 4.2 Tryb pełnego diffu

Uruchamiany, gdy:
- target nie istnieje albo nie ma `SetTransaction` dla `appId` (pierwsze uruchomienie,
  wygasła pozycja przez `delta.setTransactionRetentionDuration`, odtworzone źródło),
- w logu źródła brakuje JSON-ów z zakresu (`delta.logRetentionDuration`, domyślnie 30 dni),
- `lastSynced > S` (synchronizacja do starszej wersji przez time travel).

```
keyCols = path, coalesce(DeletionVectorDescriptor.uniqueIdExpression(deletionVector, root, false), "")
filesToAdd    = source.allFiles  LEFT ANTI JOIN target.allFiles ON keyCols
filesToRemove = target.allFiles  LEFT ANTI JOIN source.allFiles ON keyCols   -> removeWithTimestamp()
```

Wynik jest zbierany na driverze, tak jak dziś robi to `handleClone` (`collectAsList`).
Pełny diff działa też dla niepustego targetu, który nie powstał przez DEEP CLONE:
wtedy `CREATE OR REPLACE ... DEEP CLONE` zastępuje zawartość (zamiast dzisiejszego
`DELTA_UNSUPPORTED_NON_EMPTY_CLONE`, które zostaje tylko dla SHALLOW).

## 5. Kopiowanie plików

Nowy helper, np. `DeepCloneUtils.copyFiles(spark, sourcePath, targetPath, files, hadoopConf)`.

1. Lista par `(src, dst)`:
   - plik danych: `DeltaFileOperations.absolutePath(sourcePath, add.path)` ->
     `DeltaFileOperations.absolutePath(targetPath, add.path)`,
   - DV na dysku (`storageType` `u` lub `r`): `dv.absolutePath(sourcePath)` -> `dv.absolutePath(targetPath)`.
     Deskryptor DV w `AddFile` zostaje bez zmian, bo jest względny.
   - DV inline (`i`): nic do kopiowania.
   - deduplikacja par, bo wiele plików danych może współdzielić jeden plik DV
     (inaczej dwa taski pisałyby ten sam plik równolegle).
2. `spark.sparkContext.parallelize(pairs, numSlices)` z broadcastem
   `SerializableConfiguration`, `numSlices = min(pairs.size, parallelism)`.
   Parallelism z nowej konfiguracji (sekcja 7.5).
3. Dla każdej pary:
   - `srcStatus = srcFs.getFileStatus(src)`,
   - jeśli `dst` istnieje i ma tę samą długość -> pomiń (ponowienie po awarii, ponowne AddFile),
   - w przeciwnym razie `FileUtil.copy(srcFs, src, dstFs, dst, deleteSource = false, overwrite = true, conf)`.
4. Kopiowanie jest strumieniowe przez Hadoop `FileSystem`, więc działa między dowolnymi
   schematami (`s3a://` -> `abfss://`, `gs://`, lokalne), o ile na klastrze są skonfigurowane
   oba systemy plików i poświadczenia.
5. Metryki: liczba skopiowanych plików i bajtów (bez pominiętych), liczba pominiętych.

Uwaga: plik skopiowany częściowo (przerwany upload) na object store zwykle nie pojawia się
wcale, bo upload jest atomowy przy `close()`. Na systemach nieatomowych sprawdzenie długości
wyłapuje przypadek obciętego pliku.

## 6. Walidacje i ograniczenia

| Warunek | Zachowanie |
|---|---|
| Źródło nie jest tabelą Delta | Błąd `DELTA_DEEP_CLONE_UNSUPPORTED_SOURCE` (nowy). |
| `AddFile.path` absolutny (źródło samo jest shallow clonem) | Błąd `DELTA_DEEP_CLONE_ABSOLUTE_PATH_UNSUPPORTED` (nowy). |
| DV z absolutną ścieżką (`storageType = p`) | Ten sam błąd. |
| Target względny (`targetPath` nie jest absolutny) | Istniejący `cloneOnRelativePath`. |
| Clone na samego siebie | Istniejący no-op w `DeltaAnalysis`. |
| Zmiana trybu column mapping | Istniejące `checkColumnMappingMode`. |
| Równoległe synchronizacje tego samego targetu | Optimistic concurrency: obie czytają `txnVersion(appId)` i zapisują `SetTransaction`, druga dostaje `ConcurrentTransactionException`. |
| Ktoś zapisuje do targetu poza synchronizacją | Nie jest blokowane. Tryb przyrostowy porównuje z faktycznym stanem targetu tylko dotknięte ścieżki, więc obce pliki zostają. Pełny diff (np. `CREATE OR REPLACE` po wygaśnięciu logu) je usunie. Dokumentujemy: target traktujemy jako tylko do odczytu. |

Nowe klasy błędów w `spark/src/main/resources/error/delta-error-classes.json`
i fabryki w `DeltaErrors.scala`. `DELTA_UNSUPPORTED_DEEP_CLONE` usuwamy wraz z testem
w `DeltaErrorsSuite` i testem "deep clone not supported yet" w `CloneTableScalaSuite`.

## 7. Zmiany w plikach

### 7.1 Gramatyka i parser
- `DeltaSqlBase.g4`:
  - `cloneTableHeader cloneType=(SHALLOW | DEEP) CLONE source=... #clone`,
  - token `DEEP: 'DEEP';`, dopisany do `nonReserved`.
- `DeltaSqlParser.visitClone`: `isShallow = ctx.cloneType.getType == DeltaSqlBaseParser.SHALLOW`,
  przekazanie do `CloneTableStatement`, aktualizacja scaladoc.

### 7.2 Plan logiczny i analiza
- `CloneTableStatement`: nowe pole `isShallow: Boolean` (bez wartości domyślnej, żeby
  kompilator wskazał każde miejsce użycia).
- `PreprocessTimeTravel`: dodatkowe `_` w pattern matchu.
- `DeltaAnalysis.resolveCloneCommand`: przekazanie `statement.isShallow` do wszystkich
  5 konstrukcji `CloneTableCommand`.
- `DeltaTableOperations.executeClone`: usunięcie `unsupportedDeepCloneException`,
  przekazanie `isShallow` do `CloneTableStatement`.

### 7.3 Komenda
- `CloneTableCommand`: nowe pole `isShallow: Boolean`, walidacja źródła dla DEEP,
  przekazanie `isShallow` do `DeltaOperations.Clone`.
- `CloneTableBase.handleClone`: rozgałęzienie shallow / deep.
  - Shallow: bez zmian zachowania.
  - Deep: kroki z sekcji 4, wywołania helperów z `DeepCloneUtils`.
  - Metryki: `NUM_COPIED_FILES`, `COPIED_FILES_SIZE`, `NUM_REMOVED_FILES`, `REMOVED_FILES_SIZE`
    wypełniane faktycznymi wartościami dla DEEP.
  - Event `delta.clone` dostaje dodatkowe pola: `isShallow`, `syncMode`
    (`full` / `incremental` / `noop`), `lastSyncedVersion`.
- Nowy plik `commands/DeepCloneUtils.scala`:
  - `syncAppId(sourceTableId)`,
  - `computeIncrementalChanges(...)`: `Option[FileChanges]`,
  - `computeFullDiff(...)`: `FileChanges`,
  - `validateRelativePaths(files)`,
  - `copyFiles(...)`: `CopyMetrics`,
  - `case class FileChanges(filesToAdd: Seq[AddFile], filesToRemove: Seq[RemoveFile], mode: String)`.

### 7.4 Operacja w logu
- `DeltaOperations.Clone`: nowy parametr `isShallow: Boolean = true`,
  w `parameters` dochodzi `"isShallow" -> isShallow`.
  Poprawka `CommitInfoSerializerSuite`, jeśli porównuje dokładne parametry.

### 7.5 Konfiguracja (`DeltaSQLConf`)
- `spark.databricks.delta.clone.deep.copyParallelism` (int, domyślnie np. 200):
  maksymalna liczba partycji jobu kopiującego.
- `spark.databricks.delta.clone.deep.incremental.enabled` (bool, domyślnie `true`, internal):
  wyłącznik trybu przyrostowego (wymusza pełny diff), przydatny w testach i awaryjnie.

### 7.6 Dokumentacja
- `docs/`: sekcja o DEEP CLONE w dokumentacji clone: składnia, semantyka synchronizacji,
  retencja logu źródła, `VACUUM` targetu, target tylko do odczytu, brak CDF 1:1.

## 8. Retencja i utrzymanie

- **Log źródła** (`delta.logRetentionDuration`, domyślnie 30 dni): jeśli synchronizacja
  nie uruchomi się dłużej niż retencja, przyrostowy tryb nie ma danych i następuje pełny diff.
  Poprawność zachowana, koszt rośnie.
- **VACUUM źródła**: pliki usunięte w źródle mogą zniknąć fizycznie. Tryb przyrostowy ich
  nie kopiuje (są usunięte w stanie netto), pełny diff też nie (nie ma ich w snapshocie).
  Problem pojawia się tylko przy time travel do starej wersji, której pliki zostały
  wyczyszczone. Wtedy kopiowanie kończy się `FileNotFoundException`, który opakowujemy
  w czytelny błąd.
- **SetTransaction targetu** (`delta.setTransactionRetentionDuration`, domyślnie brak):
  jeśli ustawione, pozycja może wygasnąć, co powoduje pełny diff.
- **VACUUM targetu**: normalnie usuwa pliki zdjęte przez synchronizację i osierocone
  pliki z nieudanych prób.

## 9. Testy

Nowy suite `DeepCloneSuite` (lub rozszerzenie `CloneTableSuiteBase`) z lokalnymi katalogami
jako "dwiema chmurami". Każdy test porównuje dane targetu ze źródłem (`checkAnswer`)
i sprawdza `txnVersion(appId)`.

Parser (`DeltaSqlParserSuite`):
1. `CREATE TABLE t DEEP CLONE s`, `CREATE OR REPLACE ... DEEP CLONE`, z `VERSION AS OF`,
   `TBLPROPERTIES`, `LOCATION`. `checkCloneStmt` dostaje parametr `isShallow`.
2. `DEEP` jako nazwa kolumny lub tabeli nadal działa (nonReserved).

Pierwsza synchronizacja:
3. Tabela niepartycjonowana i partycjonowana: pliki fizycznie w targecie, ścieżki względne,
   brak odwołań do katalogu źródła (usunięcie katalogu źródła nie psuje odczytu targetu).
4. `CREATE TABLE` (bez REPLACE) na istniejący target: błąd jak dziś.
5. Tabela metastore i tabela po ścieżce jako target.

Synchronizacja przyrostowa:
6. Append w źródle, druga synchronizacja kopiuje tylko nowe pliki (metryka `numCopiedFiles`).
7. `DELETE` / `UPDATE` / `MERGE` bez DV: usunięte i dodane pliki.
8. To samo z DV: nowe pliki DV skopiowane, stare wpisy usunięte.
9. `OPTIMIZE` w źródle.
10. Plik dodany i usunięty między synchronizacjami: nie jest kopiowany.
11. Zmiana schematu (`ADD COLUMN`) i właściwości tabeli: metadata targetu zaktualizowana.
12. Upgrade protokołu w źródle (np. włączenie DV).
13. Brak zmian w źródle: brak nowego commitu w targecie.
14. `RESTORE` w źródle między synchronizacjami.
15. `CREATE OR REPLACE TABLE` na źródle (zastąpienie danych, to samo id).

Fallback na pełny diff:
16. Usunięte JSON-y logu źródła (symulacja retencji): wynik poprawny, `syncMode = full`.
17. Konfiguracja `incremental.enabled = false`: wynik identyczny jak w trybie przyrostowym.
18. Time travel wstecz (`VERSION AS OF` starsza niż `lastSynced`).
19. Odtworzone źródło (nowe `metadata.id`) pod tą samą ścieżką.
20. Niepusty target nieutworzony przez DEEP CLONE: zawartość zastąpiona.

Odporność:
21. Awaria po skopiowaniu plików, a przed commitem (hook / wstrzyknięty wyjątek):
    ponowne uruchomienie kończy się sukcesem, pliki nie są kopiowane drugi raz.
22. Równoległy commit do targetu w trakcie synchronizacji: konflikt.

Walidacje:
23. Źródło shallow clone (absolutne ścieżki): `DELTA_DEEP_CLONE_ABSOLUTE_PATH_UNSUPPORTED`.
24. Źródło Parquet: `DELTA_DEEP_CLONE_UNSUPPORTED_SOURCE`.

API:
25. `DeltaTable.clone(target, isShallow = false, replace = true)` w Scali, dwukrotnie.
26. Python `DeltaTable.clone(...)` z domyślnym `isShallow=False` (`python/delta/tests/test_deltatable.py`).

Opcjonalnie: włączenie DEEP w `CloneTableTestMixin.testAllClones` / `cloneTypeStr`, żeby
istniejące testy clone przebiegły dla obu trybów. Wymaga dostosowania asercji
(`delta.clone.makeAbsolute`, `setTransactions.isEmpty`, `numAbsolutePathsInAdd`).

## 10. Kolejność implementacji (osobne commity)

1. Gramatyka + parser + `CloneTableStatement.isShallow` + `PreprocessTimeTravel` + testy parsera.
   DEEP nadal rzuca błąd na etapie wykonania.
2. `DeltaAnalysis` / `DeltaTableOperations` / `CloneTableCommand.isShallow`,
   nowe klasy błędów, `DeltaOperations.Clone.isShallow`.
3. `DeepCloneUtils.copyFiles` + pełny diff + `SetTransaction`. Na tym etapie każde
   uruchomienie to pełny diff, co już daje poprawną synchronizację.
4. Tryb przyrostowy z fallbackiem + konfiguracje.
5. Pominięcie commitu przy braku zmian, metryki, eventy.
6. Pełen zestaw testów z sekcji 9 + Python.
7. Dokumentacja.

## 11. Następny etap: filtrowanie partycji (szkic)

```sql
CREATE OR REPLACE TABLE target DEEP CLONE source
  WHERE event_date >= current_date() - INTERVAL 14 DAYS
```

- Predykat tylko na kolumnach partycji, oceniany na `partitionValues`
  (`DeltaLog.filterFileList`), liczony w momencie synchronizacji, więc okno się przesuwa.
- Przyrostowo: ignorujemy `AddFile` spoza predykatu i dodatkowo w każdej rundzie usuwamy
  z targetu pliki, które przestały go spełniać (tanie, bo target jest mały).
- Tryb przyrostowy jest poprawny dla predykatów "monotonicznych" (partycja, która wypadła
  z okna, nie wraca). Zmiana tekstu predykatu (hash zapisany w `commitInfo`) wymusza pełny diff.
- Pierwsza synchronizacja: pełny diff z predykatem, czyta stan źródła, ale kopiuje tylko
  pasujące partycje.

## 12. Otwarte pytania

1. Czy przy braku zmian na pewno pomijać commit, czy jednak zapisywać pusty commit
   jako ślad "synchronizacja się odbyła" (np. do monitoringu)?
2. Czy pełny diff na niepustym, obcym targecie ma zastępować zawartość (propozycja),
   czy rzucać błąd jak SHALLOW?
3. Czy potrzebny jest tryb "dry run" (wypisanie liczby plików do skopiowania i usunięcia
   bez wykonania)?
4. Czy wymagana jest walidacja "target tylko do odczytu" (błąd, gdy od ostatniej
   synchronizacji w targecie są obce commity z danymi)?
