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
    Assert.AreEqual(1L, outLine["requestId"].Value<int64>())
    Assert.AreEqual(classFile.Uri, outLine["uri"].Value<string>())

    let result = outLine["result"] :?> JObject
    Assert.AreEqual(false, result["stale"].Value<bool>())

    let defsByCallSite = result["defsByCallSite"] :?> JArray
    Assert.AreEqual(1, defsByCallSite.Count)

    let defs0 = (defsByCallSite[0] :?> JObject)["defs"] :?> JArray
    Assert.IsTrue(defs0.Count >= 1)

    let hasMethodADef =
        defs0
        |> Seq.cast<JToken>
        |> Seq.exists (fun t ->
            let loc = t :?> JObject
            let range = loc["range"] :?> JObject
            let start = range["start"] :?> JObject
            loc["uri"].Value<string>() = classFile.Uri && start["line"].Value<int>() = 2)

    Assert.IsTrue(hasMethodADef)

    let defsByRefSite = result["defsByRefSite"] :?> JArray
    Assert.AreEqual(1, defsByRefSite.Count)

    let refDefs0 = (defsByRefSite[0] :?> JObject)["defs"] :?> JArray
    Assert.IsTrue(refDefs0.Count >= 1)

    let hasArgParamDef =
        refDefs0
        |> Seq.cast<JToken>
        |> Seq.exists (fun t ->
            let loc = t :?> JObject
            let range = loc["range"] :?> JObject
            let start = range["start"] :?> JObject
            loc["uri"].Value<string>() = classFile.Uri && start["line"].Value<int>() = 7)

    Assert.IsTrue(hasArgParamDef)
