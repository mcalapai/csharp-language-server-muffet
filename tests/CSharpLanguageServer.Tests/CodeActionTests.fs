module CSharpLanguageServer.Tests.CodeActionTests

open System

open NUnit.Framework
open Ionide.LanguageServerProtocol.Types

open CSharpLanguageServer.Tests.Tooling

[<TestCase("net10.0")>]
[<TestCase("net8.0")>]
let ``code action menu appears on request`` (tfm: string) =
    if tfm = "net8.0" && not (dotnetHasSdkMajor 8) then
        Assert.Ignore("net8.0 code action expectations require the .NET 8 SDK/targeting pack to be installed.")

    let patchFixture = patchFixtureWithTfm tfm

    use client =
        activateFixtureExt "genericProject" defaultClientProfile patchFixture id

    use classFile = client.Open "Project/Class.cs"

    let caParams: CodeActionParams =
        { TextDocument = { Uri = classFile.Uri }
          Range =
            { Start = { Line = 1u; Character = 0u }
              End = { Line = 1u; Character = 0u } }
          Context =
            { Diagnostics = [||]
              Only = None
              TriggerKind = None }
          WorkDoneToken = None
          PartialResultToken = None }

    let caResult: TextDocumentCodeActionResult option =
        client.Request("textDocument/codeAction", caParams)

    let resolveCodeAction (ca: CodeAction) : CodeAction =
        client.Request("codeAction/resolve", ca)

    let assertCodeActionHasTitle (ca: CodeAction, title: string) =
        Assert.AreEqual(title, ca.Title)
        Assert.AreEqual(None, ca.Kind)
        Assert.AreEqual(None, ca.Diagnostics)
        Assert.AreEqual(None, ca.Disabled)
        ()

    match tfm, Environment.OSVersion.Platform with
    | "net8.0", PlatformID.Win32NT -> () // this particular variant fails consistently as of Roslyn 5.0.0

    | _, _ ->
        let codeActions =
            match caResult with
            | None -> failwith "Expected code actions"
            | Some xs ->
                xs
                |> Array.choose (function
                    | U2.C2 ca -> Some ca
                    | _ -> None)

        let requireByTitle title =
            match codeActions |> Array.tryFind (fun ca -> ca.Title = title) with
            | Some ca -> ca
            | None ->
                let titles = codeActions |> Array.map _.Title |> String.concat ", "
                failwithf "Missing code action '%s'. Got: [%s]" title titles

        // Listing should include these (resolution is tested elsewhere and may vary for interactive actions).
        requireByTitle "Generate overrides..." |> fun ca -> assertCodeActionHasTitle (ca, "Generate overrides...")
        requireByTitle "Extract interface..." |> fun ca -> assertCodeActionHasTitle (ca, "Extract interface...")
        requireByTitle "Generate constructor 'Class()'" |> fun ca -> assertCodeActionHasTitle (ca, "Generate constructor 'Class()'")
        requireByTitle "Extract base class..." |> fun ca -> assertCodeActionHasTitle (ca, "Extract base class...")
        requireByTitle "Add 'DebuggerDisplay' attribute" |> fun ca -> assertCodeActionHasTitle (ca, "Add 'DebuggerDisplay' attribute")

[<Test>]
let ``extract base class request extracts base class`` () =
    use client = activateFixture "genericProject"
    use classFile = client.Open("Project/Class.cs")

    let caParams0: CodeActionParams =
        { TextDocument = { Uri = classFile.Uri }
          Range =
            { Start = { Line = 2u; Character = 16u }
              End = { Line = 2u; Character = 16u } }
          Context =
            { Diagnostics = [||]
              Only = None
              TriggerKind = None }
          WorkDoneToken = None
          PartialResultToken = None }

    let caResult: TextDocumentCodeActionResult option =
        client.Request("textDocument/codeAction", caParams0)

    match caResult with
    | Some [| U2.C2 x |] ->
        let x: CodeAction = client.Request("codeAction/resolve", x)
        Assert.AreEqual("Extract base class...", x.Title)
    // TODO: match extract base class edit structure

    | _ -> failwith "Some [| U2.C2 x |] was expected"

[<Test>]
let ``extract interface code action should extract an interface`` () =
    use client = activateFixture "genericProject"
    use classFile = client.Open("Project/Class.cs")

    let caArgs: CodeActionParams =
        { TextDocument = { Uri = classFile.Uri }
          Range =
            { Start = { Line = 0u; Character = 0u }
              End = { Line = 0u; Character = 0u } }
          Context =
            { Diagnostics = [||]
              Only = Some [| "refactor.extract.interface" |]
              TriggerKind = Some CodeActionTriggerKind.Invoked }
          WorkDoneToken = None
          PartialResultToken = None }

    let caOptions: TextDocumentCodeActionResult =
        match client.Request("textDocument/codeAction", caArgs) with
        | Some opts -> opts
        | None -> failwith "Expected code actions"

    let codeAction: CodeAction =
        let cas =
            caOptions
            |> Array.choose (function
                | U2.C2 ca -> Some ca
                | _ -> None)

        match cas |> Array.tryFind (fun ca -> ca.Title = "Extract interface...") with
        | Some ca -> (client.Request("codeAction/resolve", ca) : CodeAction)
        | None ->
            let titles = cas |> Array.map _.Title |> String.concat ", "
            failwithf "Extract interface action not found. Got: [%s]" titles

    let expectedImplementInterfaceEdits =
        { Range =
            { Start = { Line = 0u; Character = 11u }
              End = { Line = 0u; Character = 11u } }
          NewText = " : IClass" }

    let expectedCreateInterfaceEdits =
        { Range =
            { Start = { Line = 0u; Character = 0u }
              End = { Line = 0u; Character = 0u } }
          NewText = "internal interface IClass\n{\n    void MethodA(string arg);\n    void MethodB(string arg);\n}" }

    match codeAction.Edit with
    | Some { DocumentChanges = Some [| U4.C1 create; U4.C1 implement |] } ->
        match create.Edits, implement.Edits with
        | [| U2.C1 createEdits |], [| U2.C1 implementEdits |] ->
            Assert.AreEqual(expectedCreateInterfaceEdits, createEdits |> TextEdit.normalizeNewText)

            Assert.AreEqual(expectedImplementInterfaceEdits, implementEdits |> TextEdit.normalizeNewText)

        | _ -> failwith "Expected exactly one U2.C1 edit in both create/implement"

    | _ -> failwith "Unexpected edit structure"

[<Test>]
let ``code actions are listed when activated on a string literal`` () =
    use client = activateFixture "genericProject"
    use classFile = client.Open "Project/Class.cs"

    let caParams: CodeActionParams =
        { TextDocument = { Uri = classFile.Uri }
          Range =
            { Start = { Line = 4u; Character = 20u }
              End = { Line = 4u; Character = 20u } }
          Context =
            { Diagnostics = [||]
              Only = None
              TriggerKind = None }
          WorkDoneToken = None
          PartialResultToken = None }

    let caResult: TextDocumentCodeActionResult =
        match client.Request("textDocument/codeAction", caParams) with
        | Some caResult -> caResult
        | None -> failwith "Some TextDocumentCodeActionResult was expected"

    let cas =
        caResult
        |> Array.choose (function
            | U2.C2 ca -> Some ca
            | _ -> None)

    // Keep this test stable across Roslyn versions/TFMs:
    // - do not assert a fixed count
    // - do not assume ordering
    // - assert that at least one of the expected "introduce ..." refactorings is present
    let titles = cas |> Array.map _.Title

    let hasTitlePrefix (prefix: string) =
        titles |> Array.exists (fun t -> t.StartsWith(prefix, StringComparison.Ordinal))

    Assert.IsTrue(
        hasTitlePrefix "Introduce constant"
        || hasTitlePrefix "Introduce parameter",
        sprintf
            "Expected an introduce-constant or introduce-parameter refactoring. Got: [%s]"
            (String.concat ", " titles)
    )

    ()
