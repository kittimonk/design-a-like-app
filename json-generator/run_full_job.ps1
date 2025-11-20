param(
    [Parameter(Mandatory = $true)]
    [string]$JobId,

    [Parameter(Mandatory = $false)]
    [string]$Malcode = "mcb",

    [Parameter(Mandatory = $false)]
    [string]$CsvPath = "json-generator/data/source_target_mapping_clean_v9_fixed (3).csv",

    [Parameter(Mandatory = $false)]
    [string]$OutDir = "json-generator/generated_out"
)

Write-Host "===================================================="
Write-Host "  Running Full SQL Job Pipeline"
Write-Host "  Malcode : $Malcode"
Write-Host "  Job ID  : $JobId"
Write-Host "===================================================="

# Folder where this script is located
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition

function Resolve-InputPath($Path, $Base) {
    if ([System.IO.Path]::IsPathRooted($Path)) {
        return $Path
    }
    # If the incoming path already begins with json-generator/, do NOT prepend Base again
    if ($Path -like "json-generator/*") {
        return Join-Path $Base $Path.Substring(15)
    }
    return Join-Path $Base $Path
}

# FIX: resolve paths properly
$Runner = Join-Path $ScriptRoot "src/run_full_job_v1.py"
$Csv    = Resolve-InputPath $CsvPath $ScriptRoot
$Out    = Resolve-InputPath $OutDir   $ScriptRoot

Write-Host "Runner script : $Runner"
Write-Host "CSV input     : $Csv"
Write-Host "Output folder : $Out"
Write-Host ""

# Execute
python "$Runner" `
    --csv "$Csv" `
    --malcode "$Malcode" `
    --job-id "$JobId" `
    --outdir "$Out"

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n===================================================="
    Write-Host "   ✅ Pipeline completed successfully!"
    Write-Host "   Output generated in: $Out"
    Write-Host "===================================================="
} else {
    Write-Host "`n===================================================="
    Write-Host "   ❌ Pipeline FAILED with exit code $LASTEXITCODE"
    Write-Host "===================================================="
}
