# ─────────────────────────────────────────────
# PredictIQ - Deploy Script (Fast Whitelist Sync)
# Only syncs known CODE folders - never touches
# data/, .venv/, or logs/. Scans are fast because
# we only diff the small folders we care about.
# ─────────────────────────────────────────────

$PROD_IP = "100.67.60.86"
$SOURCE  = "C:\Data Engineering\codeprep\predection_project"
$DEST    = "\\$PROD_IP\predection_project"

# ── WHITELIST: Only these folders/files get synced ──
# Add new folders here as the project grows.
$SYNC_FOLDERS = @(
    "ingestion",
    "transformation",
    "orchestration"
)
$SYNC_FILES = @(
    "docker-compose.yml",
    "Dockerfile",
    "requirements.txt"
)

$EXCLUDE_FILES = @("*.pyc", "*.log")

# ── Helper ───────────────────────────────────
function Write-Header($text, $color = "Cyan") {
    Write-Host ""
    Write-Host ("=" * 52) -ForegroundColor $color
    Write-Host "  $text" -ForegroundColor $color
    Write-Host ("=" * 52) -ForegroundColor $color
    Write-Host ""
}

Write-Header "PredictIQ - Deploy to Prod"

# ── Step 1: Ping Prod ─────────────────────────
Write-Host "Checking Prod connectivity (Tailscale)..." -ForegroundColor Yellow
$ping = Test-Connection -ComputerName $PROD_IP -Count 1 -Quiet
if (-not $ping) {
    Write-Host "ERROR: Cannot reach Prod at $PROD_IP." -ForegroundColor Red
    Write-Host "       Is Tailscale running on both machines?" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}
Write-Host "Prod is reachable!" -ForegroundColor Green

# ── Step 2: Fast Dry-Run (whitelist only) ────
Write-Host ""
Write-Host "Scanning for changed files (code-only, fast)..." -ForegroundColor Yellow
Write-Host ""

$allChanges = @()

foreach ($folder in $SYNC_FOLDERS) {
    $src = "$SOURCE\$folder"
    $dst = "$DEST\$folder"
    if (-not (Test-Path $src)) { continue }

    $output = robocopy $src $dst /MIR /L /NP /NJH /NJS /XF $EXCLUDE_FILES 2>&1
    $changed = $output | Where-Object {
        $_ -match "\S" -and $_ -notmatch "^\s*\d+\s*\\$"
    } | ForEach-Object { "[$folder] $($_.Trim())" }
    $allChanges += $changed
}

foreach ($file in $SYNC_FILES) {
    $srcFile = "$SOURCE\$file"
    $dstFile = "$DEST\$file"
    if (-not (Test-Path $srcFile)) { continue }

    $srcMod = (Get-Item $srcFile).LastWriteTime
    $dstExists = Test-Path $dstFile
    if (-not $dstExists) {
        $allChanges += "[root] NEW FILE   $file"
    } else {
        $dstMod = (Get-Item $dstFile).LastWriteTime
        if ($srcMod -gt $dstMod) {
            $allChanges += "[root] UPDATED    $file  ($srcMod)"
        }
    }
}

# ── Step 3: Show Results ─────────────────────
if ($allChanges.Count -eq 0) {
    Write-Host "No changes detected. Prod is already up to date." -ForegroundColor Green
    Read-Host "Press Enter to exit"
    exit 0
}

Write-Host "FILES THAT WILL BE SYNCED TO PROD:" -ForegroundColor White
Write-Host ("-" * 52) -ForegroundColor DarkGray
foreach ($line in $allChanges) {
    if ($line -match "NEW FILE|UPDATED") {
        Write-Host "  $line" -ForegroundColor Yellow
    } elseif ($line -match "Extra|Removing") {
        Write-Host "  $line" -ForegroundColor Red
    } else {
        Write-Host "  $line" -ForegroundColor Gray
    }
}
Write-Host ("-" * 52) -ForegroundColor DarkGray
Write-Host ""
Write-Host "Total changes: $($allChanges.Count)" -ForegroundColor White
Write-Host ""

# ── Step 4: Confirmation ─────────────────────
Write-Host "WARNING: This will overwrite files on the Prod machine." -ForegroundColor Red
Write-Host "data/, .venv/, logs/ will NOT be touched." -ForegroundColor Gray
Write-Host ""
$confirm = Read-Host "Are you sure you want to deploy to Prod? (yes/no)"

if ($confirm -ne "yes") {
    Write-Host ""
    Write-Host "Deploy cancelled. No files were changed." -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 0
}

# ── Step 5: Deploy ────────────────────────────
Write-Header "Deploying to Prod..." "Yellow"

$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$logDir    = "$SOURCE\logs\deploy"
$logFile   = "$logDir\deploy_$timestamp.log"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

"Deploy log: $timestamp" | Out-File $logFile
"Source: $SOURCE"        | Out-File $logFile -Append
"Dest:   $DEST"          | Out-File $logFile -Append
""                        | Out-File $logFile -Append

foreach ($folder in $SYNC_FOLDERS) {
    $src = "$SOURCE\$folder"
    $dst = "$DEST\$folder"
    if (-not (Test-Path $src)) { continue }
    Write-Host "  Syncing $folder..." -ForegroundColor Gray
    robocopy $src $dst /MIR /Z /NP /XF $EXCLUDE_FILES /LOG+:$logFile | Out-Null
}

foreach ($file in $SYNC_FILES) {
    $srcFile = "$SOURCE\$file"
    if (Test-Path $srcFile) {
        Write-Host "  Copying $file..." -ForegroundColor Gray
        Copy-Item -Path $srcFile -Destination "$DEST\$file" -Force
        "Copied: $file" | Out-File $logFile -Append
    }
}

Write-Header "DEPLOY COMPLETE!" "Green"
Write-Host "  All code files synced to Prod successfully." -ForegroundColor Green
Write-Host ""
Write-Host "NEXT STEP:" -ForegroundColor Yellow
Write-Host "  - Python-only changes? Containers auto-pick them up in 5 min." -ForegroundColor Gray
Write-Host "  - Changed Dockerfile or requirements.txt?" -ForegroundColor Gray
Write-Host "    Run on Prod: docker compose up -d --build" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Log saved to: $logFile" -ForegroundColor DarkGray

Read-Host "Press Enter to exit"
