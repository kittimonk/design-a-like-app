param(
    [Parameter(Mandatory = $true)]
    [string]$JobId,

    [Parameter(Mandatory = $false)]
    [string]$Malcode = "mcb",

    [Parameter(Mandatory = $false)]
    [string]$CsvPath = "data/source_target_mapping_clean_v9_fixed (3).csv",

    [Parameter(Mandatory = $false)]
    [string]$OutDir = "generated_out"
)

Write-Host "===================================================="
Write-Host "  Running Full SQL Job Pipeline"
Write-Host "  Malcode : $Malcode"
Write-Host "  Job ID  : $JobId"
Write-Host "===================================================="

# Resolve all paths relative to script location
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition

$Runner = Join-Path $ScriptRoot "src/run_full_job_v1.py"
$Csv    = Join-Path $ScriptRoot $CsvPath
$Out    = Join-Path $ScriptRoot $OutDir

Write-Host "Runner script : $Runner"
Write-Host "CSV input     : $Csv"
Write-Host "Output folder : $Out"
Write-Host ""

# Execute the pipeline
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
