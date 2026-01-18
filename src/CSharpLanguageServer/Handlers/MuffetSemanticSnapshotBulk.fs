namespace CSharpLanguageServer.Handlers

open System
open System.Diagnostics
open System.IO
open System.Text
open System.Threading
open System.Globalization

open Microsoft.CodeAnalysis
open Microsoft.CodeAnalysis.Text
open Microsoft.CodeAnalysis.FindSymbols
open Microsoft.Extensions.Logging
open Ionide.LanguageServerProtocol.Types
open Ionide.LanguageServerProtocol.JsonRpc

open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Newtonsoft.Json.Serialization

open CSharpLanguageServer.Logging
open CSharpLanguageServer.State
open CSharpLanguageServer.State.ServerState
open CSharpLanguageServer.Lsp.Workspace
open CSharpLanguageServer.Roslyn.Conversions
open CSharpLanguageServer.Roslyn.Solution
open CSharpLanguageServer.Types

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
        | JTokenType.Integer ->
            let v = (t :?> JValue).Value
            match v with
            | null -> Error $"field '{name}' must be an integer"
            | :? int64 as i -> Ok i
            | :? int32 as i -> Ok(int64 i)
            | :? uint32 as i -> Ok(int64 i)
            | :? uint64 as i ->
                if i > uint64 Int64.MaxValue then
                    Error $"field '{name}' must be within [-9223372036854775808, 9223372036854775807]"
                else
                    Ok(int64 i)
            | :? IConvertible as c ->
                try
                    Ok(c.ToInt64(CultureInfo.InvariantCulture))
                with _ ->
                    Error $"field '{name}' must be an integer"
            | _ -> Error $"field '{name}' must be an integer"
        | _ -> Error $"field '{name}' must be an integer"

    let private asInt32 (t: JToken) (name: string) : Result<int, string> =
        match t.Type with
        | JTokenType.Integer ->
            match asInt64 t name with
            | Ok v when v < int64 Int32.MinValue || v > int64 Int32.MaxValue ->
                Error $"field '{name}' must be within [{Int32.MinValue}, {Int32.MaxValue}]"
            | Ok v -> Ok(int v)
            | Error e -> Error e
        | _ -> Error $"field '{name}' must be an integer"

    let private asUInt32 (t: JToken) (name: string) : Result<uint32, string> =
        match t.Type with
        | JTokenType.Integer ->
            match asInt64 t name with
            | Ok v when v < 0L || v > int64 UInt32.MaxValue ->
                Error $"field '{name}' must be within [0, {UInt32.MaxValue}]"
            | Ok v -> Ok(uint32 v)
            | Error e -> Error e
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
                                // Apply full text deterministically for this bulk line even if the version
                                // matches a previously-opened document.
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
                            | Some docFilePath ->
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
                        | _, _ -> return Error $"failed to resolve document for uri '{uri}'"
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

            let options = p.options
            let maxTargetsPerSite = options |> Option.bind _.maxTargetsPerSite |> Option.defaultValue 8
            let deadlineMs = options |> Option.bind _.deadlineMs |> Option.defaultValue 50

            let summary (files: int) (errs: int) (err: string option) =
                LspResult.success
                    { files = files
                      errors = errs
                      wallMs = int started.ElapsedMilliseconds
                      error = err }

            if String.IsNullOrWhiteSpace(requestPath) || String.IsNullOrWhiteSpace(responsePath) then
                return summary 0 1 (Some "requestPath and responsePath must be non-empty")
            elif not (Path.IsPathRooted(requestPath)) || not (Path.IsPathRooted(responsePath)) then
                return summary 0 1 (Some "requestPath and responsePath must be absolute paths")
            elif maxTargetsPerSite <= 0 then
                return summary 0 1 (Some "options.maxTargetsPerSite must be > 0")
            elif deadlineMs <= 0 then
                return summary 0 1 (Some "options.deadlineMs must be > 0")
            else
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
                                let writeResponseLine (requestId: int64) (uri: string) (result: SemanticSnapshotResult option) (error: string option) =
                                    let mkPos (p: Ionide.LanguageServerProtocol.Types.Position) =
                                        JObject(
                                            [| JProperty("line", JValue(int p.Line))
                                               JProperty("character", JValue(int p.Character)) |]
                                        )

                                    let mkRange (r: Range) =
                                        JObject(
                                            [| JProperty("start", mkPos r.Start)
                                               JProperty("end", mkPos r.End) |]
                                        )

                                    let mkLocation (l: Location) =
                                        JObject(
                                            [| JProperty("uri", JValue(l.Uri))
                                               JProperty("range", mkRange l.Range) |]
                                        )

                                    let mkDefsAtPos (d: DefsAtPos) =
                                        let defsArr = JArray(d.defs |> List.map mkLocation |> List.toArray)
                                        JObject(
                                            [| JProperty("line", JValue(d.line))
                                               JProperty("character", JValue(d.character))
                                               JProperty("defs", defsArr) |]
                                        )

                                    let mkStats (s: SnapshotStats) =
                                        JObject(
                                            [| JProperty("totalMs", JValue(s.totalMs))
                                               JProperty("resolveMs", JValue(s.resolveMs))
                                               JProperty("returnedDefs", JValue(s.returnedDefs))
                                               JProperty("truncated", JValue(s.truncated)) |]
                                        )

                                    let mkResult (r: SemanticSnapshotResult) =
                                        JObject(
                                            [| JProperty("stale", JValue(r.stale))
                                               JProperty("currentVersion", JValue(r.currentVersion))
                                               JProperty(
                                                   "defsByCallSite",
                                                   JArray(r.defsByCallSite |> Array.map mkDefsAtPos)
                                               )
                                               JProperty(
                                                   "defsByRefSite",
                                                   JArray(r.defsByRefSite |> Array.map mkDefsAtPos)
                                               )
                                               JProperty("stats", mkStats r.stats) |]
                                        )

                                    let obj = JObject()
                                    obj["requestId"] <- JValue(requestId)
                                    obj["uri"] <- JValue(uri)

                                    match result, error with
                                    | Some r, _ -> obj["result"] <- mkResult r
                                    | None, Some e -> obj["error"] <- JValue(e)
                                    | None, None -> obj["error"] <- JValue("missing result/error")

                                    let json = JsonConvert.SerializeObject(obj, jsonSettings)
                                    respWriter.WriteLine(json)

                                match parseRequestLine line with
                                | Error e ->
                                    errors <- errors + 1
                                    writeResponseLine -1L "" None (Some e)
                                    return! loop ()
                                | Ok reqLine ->
                                    processed <- processed + 1

                                    let respForError msg =
                                        errors <- errors + 1
                                        writeResponseLine reqLine.requestId reqLine.uri None (Some msg)

                                    let respForResult result = writeResponseLine reqLine.requestId reqLine.uri (Some result) None

                                    let perLineStarted = Stopwatch.StartNew()

                                    let currentVersionStr (v: int option) =
                                        v |> Option.map (fun x -> $"lsp:{x}") |> Option.defaultValue "lsp:0"

                                    match tryUriToFilePath reqLine.uri with
                                    | Error e ->
                                        respForError e
                                        return! loop ()
                                    | Ok filePath ->
                                        match ensureUnderProjectRoot reqLine.projectRootPath filePath with
                                        | Error e ->
                                            respForError e
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

                                                respForResult result
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
                                                    respForError e
                                                    return! loop ()
                                                | Ok updatedWorkspace ->
                                                    localWorkspace <- updatedWorkspace

                                                    let resolveStarted = Stopwatch.StartNew()
                                                    let mutable returnedDefs = 0
                                                    let mutable truncated = false
                                                    let! requestCt = Async.CancellationToken

                                                    let wfForUri, docForUri = workspaceDocument localWorkspace AnyDocument reqLine.uri

                                                    match wfForUri, docForUri with
                                                    | None, _ ->
                                                        respForError $"no workspace folder matches uri '{reqLine.uri}'"
                                                        return! loop ()
                                                    | Some _, None ->
                                                        respForError $"document not found in workspace solution (uri='{reqLine.uri}')"
                                                        return! loop ()
                                                    | Some wf, Some doc ->
                                                        // Best-effort warmup: many Roslyn symbol queries pay an upfront compilation cost.
                                                        // Warm the semantic model once so the (callSites + refSites) loop stays within
                                                        // the per-line deadline more often.
                                                        try
                                                            let! _ = doc.GetSemanticModelAsync(requestCt) |> Async.AwaitTask
                                                            ()
                                                        with _ ->
                                                            ()

                                                        let! sourceText: Microsoft.CodeAnalysis.Text.SourceText =
                                                            doc.GetTextAsync(requestCt) |> Async.AwaitTask

                                                        let! syntaxRoot: Microsoft.CodeAnalysis.SyntaxNode =
                                                            doc.GetSyntaxRootAsync(requestCt) |> Async.AwaitTask

                                                        let! semanticModel: Microsoft.CodeAnalysis.SemanticModel =
                                                            doc.GetSemanticModelAsync(requestCt) |> Async.AwaitTask

                                                        let clampPosition (p: int) =
                                                            if sourceText.Length <= 0 then
                                                                0
                                                            else if p < 0 then
                                                                0
                                                            else if p >= sourceText.Length then
                                                                sourceText.Length - 1
                                                            else
                                                                p

                                                        let tryResolveSymbolAt (pos: Position) : Async<Microsoft.CodeAnalysis.ISymbol option> =
                                                            async {
                                                                let position = Position.toRoslynPosition sourceText.Lines pos |> clampPosition

                                                                let fromSemanticModel () =
                                                                    match syntaxRoot |> Option.ofObj with
                                                                    | None -> None
                                                                    | Some root ->
                                                                        let token = root.FindToken(position, findInsideTrivia = true)

                                                                        match token.Parent |> Option.ofObj with
                                                                        | None -> None
                                                                        | Some node ->
                                                                            let symbolInfo = semanticModel.GetSymbolInfo(node)
                                                                            match symbolInfo.Symbol |> Option.ofObj with
                                                                            | None ->
                                                                                if symbolInfo.CandidateSymbols.Length > 0 then
                                                                                    Some symbolInfo.CandidateSymbols[0]
                                                                                else
                                                                                    let declared = semanticModel.GetDeclaredSymbol(node)
                                                                                    declared |> Option.ofObj
                                                                            | Some s -> Some s

                                                                match fromSemanticModel () with
                                                                | Some s -> return Some s
                                                                | None ->
                                                                    let! symbol =
                                                                        SymbolFinder.FindSymbolAtPositionAsync(doc, position, requestCt)
                                                                        |> Async.AwaitTask

                                                                    return symbol |> Option.ofObj
                                                            }

                                                        let resolveDefsAtPos (pos: Position) : Async<Result<Location list, string>> =
                                                            async {
                                                                match! tryResolveSymbolAt pos with
                                                                | None -> return Ok []
                                                                | Some symbol ->
                                                                    let! locations, updatedWf =
                                                                        workspaceFolderSymbolLocations wf effectiveSettings symbol doc.Project

                                                                    localWorkspace <- updatedWf |> workspaceWithFolder localWorkspace
                                                                    return Ok locations
                                                            }

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
                                                            resolveStarted.ElapsedMilliseconds >= int64 deadlineMs

                                                        let resolveSites
                                                            (sites: 'a array)
                                                            (mkPos: 'a -> Position)
                                                            (setDefs: int -> Location list -> unit)
                                                            =
                                                            let rec go idx =
                                                                async {
                                                                    if idx >= sites.Length then
                                                                        return Ok()
                                                                    else
                                                                        // Treat deadlineMs as a soft budget for additional sites:
                                                                        // once the budget is exceeded, we stop *after* at least
                                                                        // one site has been processed for this site kind.
                                                                        if deadlineExceeded () && idx > 0 then
                                                                            truncated <- true
                                                                            return Ok()
                                                                        else
                                                                            let pos = mkPos sites[idx]

                                                                            match! resolveDefsAtPos pos with
                                                                            | Error e ->
                                                                                // treat as fatal for the whole line to avoid silent partial corruption
                                                                                return Error e
                                                                            | Ok locs ->
                                                                                let normalized = locs |> normalizeLocations maxTargetsPerSite
                                                                                returnedDefs <- returnedDefs + normalized.Length
                                                                                setDefs idx normalized

                                                                                if deadlineExceeded () then
                                                                                    truncated <- true

                                                                                return! go (idx + 1)
                                                                }

                                                            go 0

                                                        let setCallDefs idx defs =
                                                            defsByCallSite[idx] <- { defsByCallSite[idx] with defs = defs }

                                                        let setRefDefs idx defs =
                                                            defsByRefSite[idx] <- { defsByRefSite[idx] with defs = defs }

                                                        let mkCallPos (s: SnapshotSitePos) =
                                                            { Line = s.line
                                                              Character = s.character }

                                                        let mkRefPos (s: SnapshotRefSite) =
                                                            { Line = s.line
                                                              Character = s.character }

                                                        match! resolveSites reqLine.callSites mkCallPos setCallDefs with
                                                        | Error e ->
                                                            respForError e
                                                            return! loop ()
                                                        | Ok() ->
                                                            match! resolveSites reqLine.refSites mkRefPos setRefDefs with
                                                            | Error e ->
                                                                respForError e
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

                                                                respForResult result
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

                    return summary processed errors None
                with ex ->
                    logger.LogError(ex, "muffet/semanticSnapshotBulk failed")
                    return summary processed (errors + 1) (Some ex.Message)
        }
