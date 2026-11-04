<#
.SYNOPSIS
  Builds, packages and publishes a Windows release in one command.

.EXAMPLE
  .\release.ps1 -Version 0.2.1 -GitDir C:\P2P_project\p2p-aggregator-upload-worktree
  .\release.ps1 -Version 0.2.1 -DryRun     # build and package only, publish nothing

Before running: add a "## <Version>" section to CHANGELOG.md.
Requires: Rust, Node (npm install done), Git, and GitHub CLI signed in (gh auth login).
#>
param(
    [Parameter(Mandatory)][string]$Version,
    # Git checkout whose desktop-windows folder is published. Defaults to this folder's own repo.
    [string]$GitDir,
    [switch]$DryRun,
    [switch]$SkipTests,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$tag = "windows-v$Version"
$out = Join-Path $root "dist\$tag"

function Step($text) { Write-Host "`n==> $text" -ForegroundColor Cyan }

# Runs a native command and fails on a non-zero exit code (cargo writes progress to stderr).
function Run($exe, [string[]]$arguments, $dir = $root) {
    Push-Location $dir
    try {
        $previous = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        & $exe @arguments 2>&1 | ForEach-Object { "$_" } | Write-Host
        $code = $LASTEXITCODE
        $ErrorActionPreference = $previous
        if ($code -ne 0) { throw "'$exe $($arguments -join ' ')' failed with exit code $code" }
    } finally { Pop-Location }
}

function Find-Tool($name, $fallback) {
    $cmd = Get-Command $name -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    if (Test-Path $fallback) { return $fallback }
    throw "$name was not found. Install it first."
}

if ($Version -notmatch '^\d+\.\d+\.\d+$') { throw "Version must look like 1.2.3 (got '$Version')." }

Step "Checking prerequisites"
$changelog = Get-Content (Join-Path $root 'CHANGELOG.md') -Raw
$section = [regex]::Match($changelog, "(?ms)^## $([regex]::Escape($Version))\s*\r?\n(.*?)(?=^## |\z)")
if (-not $section.Success) { throw "CHANGELOG.md has no '## $Version' section. Add one first." }
$notes = $section.Groups[1].Value.Trim()

if (-not $DryRun) {
    $git = Find-Tool 'git' 'C:\Program Files\Git\cmd\git.exe'
    $gh = Find-Tool 'gh' 'C:\Program Files\GitHub CLI\gh.exe'
    Run $gh @('auth', 'status')
    if (-not $GitDir) { $GitDir = (& $git -C $root rev-parse --show-toplevel 2>$null) }
    if (-not $GitDir -or -not (Test-Path (Join-Path $GitDir '.git'))) {
        throw "No git checkout found. Pass -GitDir <path to your repository checkout>."
    }
    $existing = & $git -C $GitDir tag --list $tag
    if ($existing) { throw "Tag $tag already exists. Pick a new version." }
}

Step "Setting version $Version"
foreach ($file in 'core\Cargo.toml', 'src-tauri\Cargo.toml') {
    $path = Join-Path $root $file
    $text = Get-Content $path -Raw
    $new = [regex]::Replace($text, '(?m)^version = "[^"]+"', "version = `"$Version`"", 1)
    if ($new -ne $text) { [IO.File]::WriteAllText($path, $new) }
}
$confPath = Join-Path $root 'src-tauri\tauri.conf.json'
$conf = Get-Content $confPath -Raw
$newConf = [regex]::Replace($conf, '"version":\s*"[^"]+"', "`"version`": `"$Version`"", 1)
if ($newConf -ne $conf) { [IO.File]::WriteAllText($confPath, $newConf) }

# Running copies lock the staged exes.
Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -like "$root\stage\*" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

if (-not $SkipTests) {
    Step "Running tests"
    Run 'cargo' @('test') (Join-Path $root 'core')
    Run 'cargo' @('test', '--bin', 'p2p-daemon') (Join-Path $root 'src-tauri')
}

if (-not $SkipBuild) {
    Step "Building"
    Run 'cargo' @('build', '--release', '--bin', 'p2p-aggregator') (Join-Path $root 'core')
    Run 'cargo' @('build', '--release', '--bin', 'p2p-daemon') (Join-Path $root 'src-tauri')
    Run (Join-Path $root 'node_modules\.bin\tauri.cmd') @('build', '--no-bundle') (Join-Path $root 'src-tauri')
}

Step "Packaging"
$stage = Join-Path $root 'stage'
New-Item -ItemType Directory -Force $stage, $out | Out-Null
Copy-Item (Join-Path $root 'core\target\release\p2p-aggregator.exe') $stage -Force
Copy-Item (Join-Path $root 'src-tauri\target\release\p2p-daemon.exe') $stage -Force
Copy-Item (Join-Path $root 'src-tauri\target\release\p2p-desktop.exe') $stage -Force

$exes = Get-ChildItem "$stage\*.exe"
foreach ($exe in $exes) {
    $built = $exe.VersionInfo.ProductVersion
    if ($exe.Name -ne 'p2p-aggregator.exe' -and $built -ne $Version) {
        throw "$($exe.Name) reports version '$built', expected $Version. Run without -SkipBuild."
    }
}

$zip = Join-Path $out "mesh-station-windows-$Version.zip"
Remove-Item $zip -ErrorAction SilentlyContinue
$exes | Compress-Archive -DestinationPath $zip
$sums = foreach ($file in @($exes) + (Get-Item $zip)) {
    '{0}  {1}' -f (Get-FileHash $file.FullName -Algorithm SHA256).Hash.ToLower(), $file.Name
}
$sumsPath = Join-Path $out 'SHA256SUMS.txt'
[IO.File]::WriteAllLines($sumsPath, $sums)
$sums | Write-Host

if ($DryRun) {
    Write-Host "`nDry run complete. Nothing was published. Files are in $out" -ForegroundColor Yellow
    return
}

Step "Committing and tagging"
$target = Join-Path $GitDir 'desktop-windows'
if ((Resolve-Path $target).Path -ne (Resolve-Path $root).Path) {
    # Copy sources only; never private data, build output or dependencies.
    robocopy $root $target /E /XD target stage dist node_modules data .git /XF '*.db' | Out-Null
    if ($LASTEXITCODE -ge 8) { throw "robocopy failed with exit code $LASTEXITCODE" }
}
Run $git @('add', '--', 'desktop-windows') $GitDir
Run $git @('commit', '-m', "Release ${Version}", '-m', 'Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>') $GitDir
Run $git @('tag', '-a', $tag, '-m', "Windows desktop $Version") $GitDir

Step "Publishing"
$branch = (& $git -C $GitDir rev-parse --abbrev-ref HEAD).Trim()
Run $git @('push', 'origin', $branch, $tag) $GitDir
$notesFile = Join-Path $env:TEMP "$tag-notes.md"
$footer = "Extract all three exes into one folder and run p2p-desktop.exe. The exes are unsigned, so Windows SmartScreen may warn. Verify downloads against SHA256SUMS.txt."
[IO.File]::WriteAllText($notesFile, "$notes`n`n$footer`n")
Run $gh @('release', 'create', $tag, $zip, $sumsPath, '--title', "Windows desktop $Version", '--notes-file', $notesFile) $GitDir
Remove-Item $notesFile -ErrorAction SilentlyContinue

Write-Host "`nReleased $tag" -ForegroundColor Green
