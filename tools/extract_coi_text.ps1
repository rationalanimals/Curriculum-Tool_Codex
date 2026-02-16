$pdfPath = (Resolve-Path "COI_2025-2026.pdf").Path
$outPath = Join-Path (Get-Location) "coi_extracted.txt"
$word = New-Object -ComObject Word.Application
$word.Visible = $false
try {
    $doc = $word.Documents.Open($pdfPath, $false, $true)
    $text = $doc.Content.Text
    [System.IO.File]::WriteAllText($outPath, $text)
    $doc.Close()
    $word.Quit()
    Write-Output "Extracted to $outPath"
} catch {
    try {
        if ($doc) { $doc.Close() }
        $word.Quit()
    } catch {}
    throw
}
