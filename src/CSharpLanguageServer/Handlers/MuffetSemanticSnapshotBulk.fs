namespace CSharpLanguageServer.Handlers

open System
open System.Diagnostics
open System.IO
open System.Text
open System.Threading

open Microsoft.CodeAnalysis.Text
open Microsoft.CodeAnalysis.FindSymbols
open Ionide.LanguageServerProtocol.Types
open Ionide.LanguageServerProtocol.JsonRpc

open Newtonsoft.Json
open Newtonsoft.Json.Linq

open CSharpLanguageServer.Logging
open CSharpLanguageServer.State
open CSharpLanguageServer.State.ServerState
open CSharpLanguageServer.Lsp.Workspace
open CSharpLanguageServer.Roslyn.Conversions
open CSharpLanguageServer.Roslyn.Solution

[<RequireQualifiedAccess>]
module MuffetSemanticSnapshotBulk =
    let private logger = Logging.getLoggerByName "MuffetSemanticSnapshotBulk"

    type private SnapshotSitePos = { line: uint32; character: uint32 }

    type private SnapshotRefSite =
        { line: uint32
          character: uint32
          endLine: uint32
          endCharacter: uint32 }

    type private BulkRequestLine =
        { requestId: int64
          uri: string
          projectRootPath: string
          lspVersion: int
          text: string
          callSites: SnapshotSitePos array
          refSites: SnapshotRefSite array }

    type private DefsAtPos =
        { line: int
          character: int
          defs: Location list }

    type private SnapshotStats =
        { totalMs: int
          resolveMs: int
          returnedDefs: int
          truncated: bool }

    type private SemanticSnapshotResult =
        { stale: bool
          currentVersion: string
          defsByCallSite: DefsAtPos array
          defsByRefSite: DefsAtPos array
          stats: SnapshotStats }

    type private BulkResponseLine =
        { requestId: int64
          uri: string
          result: SemanticSnapshotResult option
          error: string option }

    let private jsonSettings =
        JsonSerializerSettings(
            NullValueHandling = NullValueHandling.Ignore,
            DefaultValueHandling = DefaultValueHandling.Include
        )

    let private jreqField (o: JObject) (name: string) : Result<JToken, string> =
        match o.TryGetValue(name) with
        | true, v when not (isNull v) -> Ok v
        | _ -> Error $"missing required field '{name}'"

    let private asString (t: JToken) (name: string) : Result<string, string> =
        match t.Type with
        | JTokenType.String -> Ok(t.Value<string>())
        | _ -> Error $"field '{name}' must be a string"

    let private asInt64 (t: JToken) (name: string) : Result<int64, string> =
        match t.Type with
        | JTokenType.Integer -> Ok(t.Value<int64>())
        | _ -> Error $"field '{name}' must be an integer"

    let private asInt32 (t: JToken) (name: string) : Result<int, string> =
        match t.Type with
        | JTokenType.Integer -> Ok(t.Value<int>())
        | _ -> Error $"field '{name}' must be an integer"

    let private asUInt32 (t: JToken) (name: string) : Result<uint32, string> =
        match t.Type with
        | JTokenType.Integer ->
            let v = t.Value<int64>()
            if v < 0L || v > int64 UInt32.MaxValue then
                Error $"field '{name}' must be within [0, {UInt32.MaxValue}]"
            else
                Ok(uint32 v)
        | _ -> Error $"field '{name}' must be an integer"

    let private asArray (t: JToken) (name: string) : Result<JArray, string> =
        match t.Type with
        | JTokenType.Array -> Ok(t :?> JArray)
        | _ -> Error $"field '{name}' must be an array"

    let private parseSitePos (t: JToken) : Result<SnapshotSitePos, string> =
        match t.Type with
        | JTokenType.Object ->
            let o = t :?> JObject
            match jreqField o "line", jreqField o "character" with
            | Ok lineT, Ok charT ->
                match asUInt32 lineT "line", asUInt32 charT "character" with
                | Ok line, Ok character -> Ok { line = line; character = character }
                | Error e, _ -> Error e
                | _, Error e -> Error e
            | Error e, _ -> Error e
            | _, Error e -> Error e
        | _ -> Error "callSites item must be an object"

    let private parseRefSite (t: JToken) : Result<SnapshotRefSite, string> =
        match t.Type with
        | JTokenType.Object ->
            let o = t :?> JObject
            match jreqField o "line", jreqField o "character", jreqField o "endLine", jreqField o "endCharacter" with
            | Ok lineT, Ok charT, Ok endLineT, Ok endCharT ->
                match asUInt32 lineT "line", asUInt32 charT "character", asUInt32 endLineT "endLine", asUInt32 endCharT "endCharacter" with
                | Ok line, Ok character, Ok endLine, Ok endCharacter ->
                    Ok
                        { line = line
                          character = character
                          endLine = endLine
                          endCharacter = endCharacter }
                | Error e, _, _, _ -> Error e
                | _, Error e, _, _ -> Error e
                | _, _, Error e, _ -> Error e
                | _, _, _, Error e -> Error e
            | Error e, _, _, _ -> Error e
            | _, Error e, _, _ -> Error e
            | _, _, Error e, _ -> Error e
            | _, _, _, Error e -> Error e
        | _ -> Error "refSites item must be an object"

    let private parseRequestLine (line: string) : Result<BulkRequestLine, string> =
        let parsedResult =
            try
                Ok(JToken.Parse(line))
            with ex ->
                Error($"invalid JSON: {ex.Message}")

        match parsedResult with
        | Error e -> Error e
        | Ok parsed ->
            match parsed.Type with
            | JTokenType.Object ->
                let o = parsed :?> JObject

                match
                    jreqField o "requestId",
                    jreqField o "uri",
                    jreqField o "projectRootPath",
                    jreqField o "lspVersion",
                    jreqField o "text",
                    jreqField o "callSites",
                    jreqField o "refSites"
                with
                | Ok requestIdT, Ok uriT, Ok projectRootT, Ok lspVersionT, Ok textT, Ok callSitesT, Ok refSitesT ->
                    match asInt64 requestIdT "requestId",
                          asString uriT "uri",
                          asString projectRootT "projectRootPath",
                          asInt32 lspVersionT "lspVersion",
                          asString textT "text",
                          asArray callSitesT "callSites",
                          asArray refSitesT "refSites" with
                    | Ok requestId, Ok uri, Ok projectRootPath, Ok lspVersion, Ok text, Ok callSitesArr, Ok refSitesArr ->
                        if lspVersion <= 0 then
                            Error "field 'lspVersion' must be > 0"
                        else
                            let parseArrayItems (arr: JArray) (parse: JToken -> Result<'a, string>) (name: string) =
                                let items = ResizeArray<'a>(arr.Count)
                                let mutable err: string option = None

                                for item in arr do
                                    if err.IsNone then
                                        match parse item with
                                        | Ok v -> items.Add(v)
                                        | Error e -> err <- Some($"{name}: {e}")

                                match err with
                                | Some e -> Error e
                                | None -> Ok(items.ToArray())

                            match parseArrayItems callSitesArr parseSitePos "callSites", parseArrayItems refSitesArr parseRefSite "refSites" with
                            | Ok callSites, Ok refSites ->
                                Ok
                                    { requestId = requestId
                                      uri = uri
                                      projectRootPath = projectRootPath
                                      lspVersion = lspVersion
                                      text = text
                                      callSites = callSites
                                      refSites = refSites }
                            | Error e, _ -> Error e
                            | _, Error e -> Error e
                    | Error e, _, _, _, _, _, _ -> Error e
                    | _, Error e, _, _, _, _, _ -> Error e
                    | _, _, Error e, _, _, _, _ -> Error e
                    | _, _, _, Error e, _, _, _ -> Error e
                    | _, _, _, _, Error e, _, _ -> Error e
                    | _, _, _, _, _, Error e, _ -> Error e
                    | _, _, _, _, _, _, Error e -> Error e
                | Error e, _, _, _, _, _, _ -> Error e
                | _, Error e, _, _, _, _, _ -> Error e
                | _, _, Error e, _, _, _, _ -> Error e
                | _, _, _, Error e, _, _, _ -> Error e
                | _, _, _, _, Error e, _, _ -> Error e
                | _, _, _, _, _, Error e, _ -> Error e
                | _, _, _, _, _, _, Error e -> Error e
            | _ -> Error "request line must be a JSON object"

    let private locationSortKey (l: Location) =
        (l.Uri,
         int l.Range.Start.Line,
         int l.Range.Start.Character,
         int l.Range.End.Line,
         int l.Range.End.Character)

    let private normalizeLocations (maxTargetsPerSite: int) (ls: Location list) : Location list =
        ls
        |> List.filter (fun l -> l.Uri.StartsWith("file://"))
        |> List.sortBy locationSortKey
        |> List.distinctBy locationSortKey
        |> fun xs ->
            if xs.Length > maxTargetsPerSite then
                xs |> List.take maxTargetsPerSite
            else
                xs

    let private tryUriToFilePath (uri: string) : Result<string, string> =
        try
            let u = Uri(uri)
            if u.Scheme <> "file" then
                Error $"uri must use file scheme (uri='{uri}')"
            else
                Ok u.LocalPath
        with ex ->
            Error $"invalid uri '{uri}': {ex.Message}"

    let private ensureUnderProjectRoot (projectRootPath: string) (filePath: string) : Result<unit, string> =
        try
            let root = Path.GetFullPath(projectRootPath).TrimEnd(Path.DirectorySeparatorChar)
            let file = Path.GetFullPath(filePath)
            let rootWithSep = root + string Path.DirectorySeparatorChar
            if file = root || file.StartsWith(rootWithSep, StringComparison.Ordinal) then
                Ok()
            else
                Error $"uri path is not under projectRootPath (projectRootPath='{root}', uriPath='{file}')"
        with ex ->
            Error $"failed to validate projectRootPath: {ex.Message}"

    let private applyFullTextIfNeeded
        (context: ServerRequestContext)
        (workspace: LspWorkspace)
        (uri: string)
        (version: int)
        (text: string)
        : Async<Result<LspWorkspace, string>> =
        async {
            match workspaceDocumentVersion workspace uri with
            | Some prev when prev > version -> return Error $"stale version: current={prev}, requested={version}"
            | Some prev when prev = version -> return Ok workspace
            | _ ->
                let wfOpt = workspaceFolder workspace uri

                match wfOpt with
                | None -> return Error $"no workspace folder matches uri '{uri}'"
                | Some wf ->
                    match wf.Solution with
                    | None -> return Error $"workspace folder solution not loaded for '{wf.Uri}'"
                    | Some sol ->
                        let wf2, docAndDocType = workspaceDocumentDetails workspace AnyDocument uri

                        match wf2, docAndDocType with
                        | Some wf2, Some(doc, docType) ->
                            match docType with
                            | UserDocument ->
                                let updatedDoc = SourceText.From(text) |> doc.WithText
                                let updatedWf = { wf2 with Solution = Some updatedDoc.Project.Solution }
                                let updatedWorkspace =
                                    updatedWf
                                    |> workspaceWithFolder workspace
                                    |> fun w ->
                                        { w with
                                            OpenDocs =
                                                w.OpenDocs
                                                |> Map.add uri { Version = version; Touched = DateTime.Now } }

                                context.Emit(DocumentOpened(uri, version, DateTime.Now))
                                context.Emit(WorkspaceFolderChange updatedWf)
                                return Ok updatedWorkspace
                            | _ -> return Error $"bulk apply refused for non-user document uri '{uri}'"
                        | Some wf2, None ->
                            let docFilePathMaybe = uri |> workspaceFolderUriToPath wf2

                            match docFilePathMaybe with
                            | None -> return Error $"failed to map uri to path (uri='{uri}')"
                            | Some docFilePath -> async {
                                let! newDocMaybe = solutionTryAddDocument docFilePath text sol

                                match newDocMaybe with
                                | None ->
                                    return Error $"no parent project resolved to add file to workspace (file='{docFilePath}')"
                                | Some newDoc ->
                                    let updatedWf = { wf2 with Solution = Some newDoc.Project.Solution }
                                    let updatedWorkspace =
                                        updatedWf
                                        |> workspaceWithFolder workspace
                                        |> fun w ->
                                            { w with
                                                OpenDocs =
                                                    w.OpenDocs
                                                    |> Map.add uri { Version = version; Touched = DateTime.Now } }

                                    context.Emit(DocumentOpened(uri, version, DateTime.Now))
                                    context.Emit(WorkspaceFolderChange updatedWf)
                                    return Ok updatedWorkspace
                              }
                        | _, _ -> return Error $"failed to resolve document for uri '{uri}'"
        }

    let private resolveDefsAt
        (workspace: LspWorkspace)
        (settings: CSharpLanguageServer.Types.ServerSettings)
        (uri: string)
        (pos: Position)
        (ct: CancellationToken)
        : Async<Result<Location list * LspWorkspace, string>> =
        async {
            let wf, docMaybe = uri |> workspaceDocument workspace AnyDocument

            match wf, docMaybe with
            | None, _ -> return Error $"no workspace folder matches uri '{uri}'"
            | Some _, None -> return Error $"document not found in workspace solution (uri='{uri}')"
            | Some wf, Some doc ->
                let! sourceText = doc.GetTextAsync(ct) |> Async.AwaitTask
                let position = Position.toRoslynPosition sourceText.Lines pos
                let! symbol = SymbolFinder.FindSymbolAtPositionAsync(doc, position, ct) |> Async.AwaitTask

                match symbol |> Option.ofObj with
                | None -> return Ok([], workspace)
                | Some symbol ->
                    let! locations, updatedWf = workspaceFolderSymbolLocations wf settings symbol doc.Project
                    let updatedWorkspace = updatedWf |> workspaceWithFolder workspace
                    return Ok(locations, updatedWorkspace)
        }

    let private emptyResultFor (currentVersion: string) (callSites: SnapshotSitePos array) (refSites: SnapshotRefSite array) =
        { stale = false
          currentVersion = currentVersion
          defsByCallSite =
            callSites
            |> Array.map (fun s ->
                { line = int s.line
                  character = int s.character
                  defs = [] })
          defsByRefSite =
            refSites
            |> Array.map (fun s ->
                { line = int s.line
                  character = int s.character
                  defs = [] })
          stats =
            { totalMs = 0
              resolveMs = 0
              returnedDefs = 0
              truncated = false } }

    let handle
        (context: ServerRequestContext)
        (p: CSharpLanguageServer.Types.MuffetSemanticSnapshotBulkParams)
        : AsyncLspResult<CSharpLanguageServer.Types.MuffetSemanticSnapshotBulkResponse> =
        async {
            let started = Stopwatch.StartNew()

            let requestPath = p.requestPath
            let responsePath = p.responsePath

            if String.IsNullOrWhiteSpace(requestPath) || String.IsNullOrWhiteSpace(responsePath) then
                return
                    { files = 0
                      errors = 1
                      wallMs = int started.ElapsedMilliseconds
                      error = Some "requestPath and responsePath must be non-empty" }
                    |> LspResult.success

            if not (Path.IsPathRooted(requestPath)) || not (Path.IsPathRooted(responsePath)) then
                return
                    { files = 0
                      errors = 1
                      wallMs = int started.ElapsedMilliseconds
                      error = Some "requestPath and responsePath must be absolute paths" }
                    |> LspResult.success

            let options = p.options
            let maxTargetsPerSite = options |> Option.bind _.maxTargetsPerSite |> Option.defaultValue 8
            let deadlineMs = options |> Option.bind _.deadlineMs |> Option.defaultValue 50

            if maxTargetsPerSite <= 0 then
                return
                    { files = 0
                      errors = 1
                      wallMs = int started.ElapsedMilliseconds
                      error = Some "options.maxTargetsPerSite must be > 0" }
                    |> LspResult.success

            if deadlineMs <= 0 then
                return
                    { files = 0
                      errors = 1
                      wallMs = int started.ElapsedMilliseconds
                      error = Some "options.deadlineMs must be > 0" }
                    |> LspResult.success

            let responseDir = Path.GetDirectoryName(responsePath)
            if not (String.IsNullOrEmpty(responseDir)) then
                Directory.CreateDirectory(responseDir) |> ignore

            let tmpResponsePath = responsePath + ".tmp"

            let effectiveSettings =
                // Bulk contract for Muffet ingestion consumes only file:// uris.
                // Avoid metadata decompilation work and always suppress metadata URIs.
                { context.State.Settings with UseMetadataUris = false }

            let mutable localWorkspace = context.Workspace
            let mutable processed = 0
            let mutable errors = 0

            try
                use reqFs = new FileStream(requestPath, FileMode.Open, FileAccess.Read, FileShare.Read)
                use reqReader = new StreamReader(reqFs, Encoding.UTF8, detectEncodingFromByteOrderMarks = true)
                use respFs =
                    new FileStream(tmpResponsePath, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 128)

                use respWriter = new StreamWriter(respFs, Encoding.UTF8)

                let rec loop () =
                    async {
                        let! line = reqReader.ReadLineAsync() |> Async.AwaitTask

                        if isNull line then
                            return ()
                        else if String.IsNullOrWhiteSpace(line) then
                            return! loop ()
                        else
                            let writeResponse (obj: BulkResponseLine) =
                                let json = JsonConvert.SerializeObject(obj, jsonSettings)
                                respWriter.WriteLine(json)

                            match parseRequestLine line with
                            | Error e ->
                                errors <- errors + 1
                                let respLine =
                                    { requestId = -1L
                                      uri = ""
                                      result = None
                                      error = Some e }
                                writeResponse respLine
                                return! loop ()
                            | Ok reqLine ->
                                processed <- processed + 1

                                let respForError msg =
                                    errors <- errors + 1
                                    { requestId = reqLine.requestId
                                      uri = reqLine.uri
                                      result = None
                                      error = Some msg }

                                let respForResult result =
                                    { requestId = reqLine.requestId
                                      uri = reqLine.uri
                                      result = Some result
                                      error = None }

                                let perLineStarted = Stopwatch.StartNew()

                                let currentVersionStr (v: int option) =
                                    v |> Option.map (fun x -> $"lsp:{x}") |> Option.defaultValue "lsp:0"

                                match tryUriToFilePath reqLine.uri with
                                | Error e ->
                                    respForError e |> writeResponse
                                    return! loop ()
                                | Ok filePath ->
                                    match ensureUnderProjectRoot reqLine.projectRootPath filePath with
                                    | Error e ->
                                        respForError e |> writeResponse
                                        return! loop ()
                                    | Ok() ->
                                        match workspaceDocumentVersion localWorkspace reqLine.uri with
                                        | Some prev when prev > reqLine.lspVersion ->
                                            let result =
                                                { emptyResultFor
                                                    (currentVersionStr (Some prev))
                                                    reqLine.callSites
                                                    reqLine.refSites with
                                                    stale = true
                                                    stats =
                                                        { totalMs = int perLineStarted.ElapsedMilliseconds
                                                          resolveMs = 0
                                                          returnedDefs = 0
                                                          truncated = false } }

                                            respForResult result |> writeResponse
                                            return! loop ()
                                        | _ ->
                                            let! maybeUpdatedWorkspace =
                                                applyFullTextIfNeeded
                                                    context
                                                    localWorkspace
                                                    reqLine.uri
                                                    reqLine.lspVersion
                                                    reqLine.text

                                            match maybeUpdatedWorkspace with
                                            | Error e ->
                                                // Note: stale version is handled above. Any other error is fatal for this line.
                                                respForError e |> writeResponse
                                                return! loop ()
                                            | Ok updatedWorkspace ->
                                                localWorkspace <- updatedWorkspace

                                                let resolveStarted = Stopwatch.StartNew()
                                                let mutable returnedDefs = 0
                                                let mutable truncated = false
                                                use resolveCts = new CancellationTokenSource()
                                                resolveCts.CancelAfter(deadlineMs)

                                                let defsByCallSite =
                                                    reqLine.callSites
                                                    |> Array.map (fun s ->
                                                        { line = int s.line
                                                          character = int s.character
                                                          defs = [] })

                                                let defsByRefSite =
                                                    reqLine.refSites
                                                    |> Array.map (fun s ->
                                                        { line = int s.line
                                                          character = int s.character
                                                          defs = [] })

                                                let inline deadlineExceeded () =
                                                    perLineStarted.ElapsedMilliseconds >= int64 deadlineMs

                                                let resolveSites<'a>
                                                    (sites: 'a array)
                                                    (mkPos: 'a -> Position)
                                                    (setDefs: int -> Location list -> unit)
                                                    =
                                                    async {
                                                        for idx = 0 to sites.Length - 1 do
                                                            if not truncated && deadlineExceeded () then
                                                                truncated <- true
                                                            if truncated then
                                                                () // leave remaining entries empty
                                                            else
                                                                let pos = mkPos sites[idx]
                                                                try
                                                                    match! resolveDefsAt localWorkspace effectiveSettings reqLine.uri pos resolveCts.Token with
                                                                    | Error e ->
                                                                        // treat as fatal for the whole line to avoid silent partial corruption
                                                                        return Error e
                                                                    | Ok(locs, updatedWorkspace2) ->
                                                                        localWorkspace <- updatedWorkspace2
                                                                        let normalized = locs |> normalizeLocations maxTargetsPerSite
                                                                        returnedDefs <- returnedDefs + normalized.Length
                                                                        setDefs idx normalized
                                                                with
                                                                | :? OperationCanceledException
                                                                | :? System.Threading.Tasks.TaskCanceledException ->
                                                                    truncated <- true
                                                        return Ok()
                                                    }

                                                let setCallDefs idx defs =
                                                    defsByCallSite[idx] <- { defsByCallSite[idx] with defs = defs }

                                                let setRefDefs idx defs =
                                                    defsByRefSite[idx] <- { defsByRefSite[idx] with defs = defs }

                                                let mkCallPos (s: SnapshotSitePos) = { Line = s.line; Character = s.character }

                                                let mkRefPos (s: SnapshotRefSite) = { Line = s.line; Character = s.character }

                                                match! resolveSites reqLine.callSites mkCallPos setCallDefs with
                                                | Error e ->
                                                    respForError e |> writeResponse
                                                    return! loop ()
                                                | Ok() ->
                                                    match! resolveSites reqLine.refSites mkRefPos setRefDefs with
                                                    | Error e ->
                                                        respForError e |> writeResponse
                                                        return! loop ()
                                                    | Ok() ->
                                                        let result =
                                                            { stale = false
                                                              currentVersion = $"lsp:{reqLine.lspVersion}"
                                                              defsByCallSite = defsByCallSite
                                                              defsByRefSite = defsByRefSite
                                                              stats =
                                                                { totalMs = int perLineStarted.ElapsedMilliseconds
                                                                  resolveMs = int resolveStarted.ElapsedMilliseconds
                                                                  returnedDefs = returnedDefs
                                                                  truncated = truncated } }

                                                        respForResult result |> writeResponse
                                                        return! loop ()
                    }

                do! loop ()
                do! respWriter.FlushAsync() |> Async.AwaitTask

                File.Move(tmpResponsePath, responsePath, true)

                logger.LogInformation(
                    "muffet/semanticSnapshotBulk complete (lines={lines}, errors={errors}, wallMs={wallMs})",
                    processed,
                    errors,
                    int started.ElapsedMilliseconds
                )

                return
                    { files = processed
                      errors = errors
                      wallMs = int started.ElapsedMilliseconds
                      error = None }
                    |> LspResult.success
            with ex ->
                logger.LogError(ex, "muffet/semanticSnapshotBulk failed")
                return
                    { files = processed
                      errors = errors + 1
                      wallMs = int started.ElapsedMilliseconds
                      error = Some ex.Message }
                    |> LspResult.success
        }
