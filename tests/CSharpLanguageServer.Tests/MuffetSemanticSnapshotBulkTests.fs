module CSharpLanguageServer.Tests.MuffetSemanticSnapshotBulkTests

open System
open System.IO

open NUnit.Framework
open Newtonsoft.Json.Linq

open CSharpLanguageServer.Types
open CSharpLanguageServer.Tests.Tooling

[<Test>]
let ``muffet/semanticSnapshotBulk writes NDJSON response`` () =
    use client = activateFixture "genericProject"
    use classFile = client.Open "Project/Class.cs"

    let requireProp (name: string) (o: JObject) : JToken =
        match o.TryGetValue(name) with
        | true, v ->
            match Option.ofObj v with
            | Some v -> v
            | None -> failwithf "missing JSON field '%s'" name
        | _ -> failwithf "missing JSON field '%s'" name

    let asJObject (t: JToken) : JObject =
        match t with
        | :? JObject as o -> o
        | _ -> failwithf "expected object JSON token, got %O" t.Type

    let asJArray (t: JToken) : JArray =
        match t with
        | :? JArray as a -> a
        | _ -> failwithf "expected array JSON token, got %O" t.Type

    let asString (t: JToken) : string =
        match t with
        | :? JValue as v -> v.Value :?> string
        | _ -> failwithf "expected string JSON token, got %O" t.Type

    let asInt (t: JToken) : int =
        match t with
        | :? JValue as v -> Convert.ToInt32(v.Value)
        | _ -> failwithf "expected int JSON token, got %O" t.Type

    let asInt64 (t: JToken) : int64 =
        match t with
        | :? JValue as v -> Convert.ToInt64(v.Value)
        | _ -> failwithf "expected int64 JSON token, got %O" t.Type

    let asBool (t: JToken) : bool =
        match t with
        | :? JValue as v -> Convert.ToBoolean(v.Value)
        | _ -> failwithf "expected bool JSON token, got %O" t.Type

    let requestPath = Path.Combine(client.SolutionDir, "muffet_semantic_snapshot_bulk.request.ndjson")
    let responsePath = Path.Combine(client.SolutionDir, "muffet_semantic_snapshot_bulk.response.ndjson")

    let lineObj =
        let o = JObject()
        o["requestId"] <- JValue(1L)
        o["uri"] <- JValue(classFile.Uri)
        o["projectRootPath"] <- JValue(client.SolutionDir)
        o["lspVersion"] <- JValue(1)
        o["text"] <- JValue(classFile.GetFileContents())

        let callSites = JArray()
        callSites.Add(JObject([| JProperty("line", JValue(9)); JProperty("character", JValue(8)) |]))
        o["callSites"] <- callSites

        let refSites = JArray()
        refSites.Add(
            JObject(
                [| JProperty("line", JValue(9))
                   JProperty("character", JValue(16))
                   JProperty("endLine", JValue(9))
                   JProperty("endCharacter", JValue(19)) |]
            )
        )
        o["refSites"] <- refSites
        o

    File.WriteAllText(requestPath, lineObj.ToString(Newtonsoft.Json.Formatting.None) + "\n")

    let bulkParams: MuffetSemanticSnapshotBulkParams =
        { requestPath = requestPath
          responsePath = responsePath
          options = Some { maxTargetsPerSite = Some 8; deadlineMs = Some 500 } }

    let response: MuffetSemanticSnapshotBulkResponse =
        client.Request("muffet/semanticSnapshotBulk", bulkParams)

    match response.error with
    | None -> ()
    | Some e -> Assert.Fail($"bulk response error: {e}")
    Assert.AreEqual(1, response.files)
    Assert.AreEqual(0, response.errors)

    let lines = File.ReadAllLines(responsePath)
    Assert.AreEqual(1, lines.Length)

    let outLine = JObject.Parse(lines[0])
    Assert.AreEqual(1L, outLine |> requireProp "requestId" |> asInt64)
    Assert.AreEqual(classFile.Uri, outLine |> requireProp "uri" |> asString)

    let result = outLine |> requireProp "result" |> asJObject
    Assert.AreEqual(false, result |> requireProp "stale" |> asBool)

    let defsByCallSite = result |> requireProp "defsByCallSite" |> asJArray
    Assert.AreEqual(1, defsByCallSite.Count)

    let defs0 =
        defsByCallSite[0] |> asJObject |> requireProp "defs" |> asJArray
    Assert.IsTrue(defs0.Count >= 1)

    let hasMethodADef =
        defs0
        |> Seq.cast<JToken>
        |> Seq.exists (fun t ->
            let loc = t |> asJObject
            let range = loc |> requireProp "range" |> asJObject
            let start = range |> requireProp "start" |> asJObject
            (loc |> requireProp "uri" |> asString) = classFile.Uri
            && (start |> requireProp "line" |> asInt) = 2)

    Assert.IsTrue(hasMethodADef)

    let defsByRefSite = result |> requireProp "defsByRefSite" |> asJArray
    Assert.AreEqual(1, defsByRefSite.Count)

    let refDefs0 =
        defsByRefSite[0] |> asJObject |> requireProp "defs" |> asJArray
    Assert.IsTrue(refDefs0.Count >= 1)

    let hasArgParamDef =
        refDefs0
        |> Seq.cast<JToken>
        |> Seq.exists (fun t ->
            let loc = t |> asJObject
            let range = loc |> requireProp "range" |> asJObject
            let start = range |> requireProp "start" |> asJObject
            (loc |> requireProp "uri" |> asString) = classFile.Uri
            && (start |> requireProp "line" |> asInt) = 7)

    Assert.IsTrue(hasArgParamDef)
